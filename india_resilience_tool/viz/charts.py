"""
Chart builders for IRT visualizations.

This module centralizes small-panel figures used across the dashboard:
- yearly trend plot (historical + scenario)
- scenario comparison bar chart

It also provides the helper to assemble the tidy scenario/period/value panel
used by the scenario comparison chart.

Streamlit-free: caching belongs in the app layer.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

from india_resilience_tool.viz.style import (
    IRTFigureStyle,
    add_ra_logo,
    ensure_16x9_figsize,
    irt_style_context,
    strip_spines,
)

PathLike = Union[str, Path]


# -------------------------
# Scenario / period helpers (index-agnostic)
# -------------------------

SCENARIO_ORDER = ["historical", "ssp245", "ssp585"]

SCENARIO_DISPLAY = {
    "historical": "Historical",
    "ssp245": "SSP2-4.5",
    "ssp585": "SSP5-8.5",
}

# Consistent scenario colors for Plotly + Matplotlib (module-level so it's always defined)
SCENARIO_COLORS_HEX: dict[str, str] = {
    "historical": "#1f77b4",  # blue
    "ssp245": "#ff7f0e",      # orange
    "ssp585": "#d62728",      # red
}

PERIOD_ORDER = ["1990-2010", "2020-2040", "2040-2060",
                #  "2060-2080",
                ]

# Human-friendly labels used in UI and chart axes (keys must match PERIOD_ORDER)
PERIOD_DISPLAY: dict[str, str] = {
    "1990-2010": "1990–2010",
    "2020-2040": "Early century (2020–2040)",
    "2040-2060": "Mid-century (2040–2060)",
    # "2060-2080": "End-century (2060–2080)",
}

def period_display_label(period_key: str) -> str:
    """Return a human-friendly period label for UI/plots."""
    key = canonical_period_label(period_key)
    return PERIOD_DISPLAY.get(key, key)


def canonical_period_label(raw: str) -> str:
    """Normalize period strings to canonical 'YYYY-YYYY'."""
    s = str(raw).strip()
    m = re.match(r"^(\d{4})\D+(\d{4})$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return s


def build_scenario_comparison_panel_for_row(
    row: pd.Series,
    schema_items: Sequence[Mapping[str, Any]],
    metric_name: str,
    sel_stat: str,
) -> pd.DataFrame:
    """
    Build a tidy table with scenario/period/value/column for the given metric.

    Mirrors the legacy dashboard behavior:
      - includes only scenarios in SCENARIO_ORDER
      - includes only periods in PERIOD_ORDER
      - skips NaN values
      - sorts by (period, scenario)
    """
    records: list[dict[str, Any]] = []

    for item in schema_items:
        if item.get("metric") != metric_name:
            continue
        if item.get("stat") != sel_stat:
            continue

        scen_raw = str(item.get("scenario", "")).strip().lower()
        if scen_raw not in SCENARIO_ORDER:
            continue

        period_raw = canonical_period_label(item.get("period", ""))
        if period_raw not in PERIOD_ORDER:
            continue

        col = item.get("column")
        if col not in row.index:
            continue

        val = row.get(col)
        try:
            val_f = float(pd.to_numeric(val, errors="coerce"))
        except Exception:
            val_f = float("nan")

        if pd.isna(val_f):
            continue

        records.append(
            {
                "scenario": scen_raw,
                "period": period_raw,
                "value": float(val_f),
                "column": col,
            }
        )

    if not records:
        return pd.DataFrame()

    dfp = pd.DataFrame(records)
    dfp["scenario_display"] = dfp["scenario"].map(SCENARIO_DISPLAY).fillna(dfp["scenario"])
    dfp["period"] = pd.Categorical(dfp["period"], PERIOD_ORDER, ordered=True)
    dfp["scenario"] = pd.Categorical(dfp["scenario"], SCENARIO_ORDER, ordered=True)
    dfp = dfp.sort_values(["period", "scenario"]).reset_index(drop=True)
    return dfp


def make_scenario_comparison_figure(
    panel_df: pd.DataFrame,
    metric_label: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    district_name: str,
    ax=None,
    figsize: Optional[tuple[float, float]] = None,
    fig_dpi: int = 150,
    font_size_title: int = 11,
    font_size_label: int = 10,
    font_size_ticks: int = 9,
    font_size_legend: int = 9,
    *,
    units: Optional[str] = None,
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
) -> Tuple[Optional[Any], Optional[Any]]:
    """
    Build a compact bar chart showing period-mean values for each scenario.

    Notes:
    - If `ax` is provided, the caller owns figure sizing; this function will not
      add the logo to avoid duplicating it on multi-axes pages (e.g., A4 PDFs).
    - `units` is optional and only affects axis labeling/value formatting.
    """
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    from india_resilience_tool.viz.formatting import format_value

    if panel_df is None or panel_df.empty:
        return None, None

    created_axes = ax is None

    s = style or IRTFigureStyle()
    figsize_eff = figsize or ensure_16x9_figsize(s.panel_figsize, mode="fit_width")

    # Canonicalise inputs for highlighting
    sel_scenario_norm = str(sel_scenario).strip().lower()
    sel_period_norm = canonical_period_label(sel_period)
    sel_stat_norm = str(sel_stat)

    dfp = panel_df.copy()
    dfp["period"] = dfp["period"].map(canonical_period_label)
    dfp["scenario_norm"] = dfp["scenario"].astype(str).str.strip().str.lower()

    combos: list[tuple[str, str]] = []
    for scen in SCENARIO_ORDER:
        scen_norm = str(scen).strip().lower()
        for period in PERIOD_ORDER:
            mask = (dfp["scenario_norm"] == scen_norm) & (dfp["period"] == period)
            if mask.any():
                combos.append((scen_norm, period))

    if not combos:
        return None, None

    periods_present: list[str] = []
    for period in PERIOD_ORDER:
        if any(p == period for (_, p) in combos):
            periods_present.append(period)

    if not periods_present:
        return None, None

    group_spacing = 2.0
    within_spacing = 0.6
    x_positions: dict[tuple[str, str], float] = {}

    for p_idx, period in enumerate(periods_present):
        scen_here = [sc for (sc, p) in combos if p == period]
        if not scen_here:
            continue
        n_scen = len(scen_here)
        start_x = p_idx * group_spacing - (n_scen - 1) * within_spacing / 2.0
        for j, scen in enumerate(scen_here):
            x_positions[(scen, period)] = start_x + j * within_spacing

    xs: list[float] = []
    ys: list[float] = []
    colors: list[str] = []
    edgecolors: list[Any] = []
    linewidths: list[float] = []
    for scen, period in combos:
        mask = (dfp["scenario_norm"] == scen) & (dfp["period"] == period)
        row = dfp.loc[mask].iloc[0]
        y = row.get("value", np.nan)

        xs.append(float(x_positions[(scen, period)]))
        ys.append(float(y) if pd.notna(y) else np.nan)

        base_color = SCENARIO_COLORS_HEX.get(scen, "tab:blue")
        colors.append(base_color)

        is_selected = (scen == sel_scenario_norm) and (period == sel_period_norm)

        # Matplotlib expects a named color or an (r, g, b, a) tuple (0–1 floats)
        edgecolors.append("black" if is_selected else (0.0, 0.0, 0.0, 0.35))
        linewidths.append(1.4 if is_selected else 0.9)

    if not xs:
        return None, None

    y_label = metric_label
    u = (units or "").strip()
    if u and f"({u})" not in y_label:
        y_label = f"{y_label} ({u})"

    with irt_style_context(s):
        if ax is None:
            fig, ax = plt.subplots(figsize=figsize_eff, dpi=fig_dpi)
        else:
            fig = ax.figure

        # `xs` is the computed x-position array; some code paths used `x` earlier.
        x = xs
        y = ys

        bars = ax.bar(
            x,
            y,
            color=colors,
            edgecolor=edgecolors,
            linewidth=linewidths,
            width=0.45,
        )

        # ---------------------------------------------------------------------
        # Auto-zoom y-axis only when the bars have a high baseline + small spread.
        # This makes small differences visible without always truncating the axis.
        # ---------------------------------------------------------------------
        y_vals = np.array([v for v in ys if pd.notna(v)], dtype=float)

        zoomed = False
        if y_vals.size >= 2:
            y_min = float(np.min(y_vals))
            y_max = float(np.max(y_vals))
            if y_max > 0:
                spread = y_max - y_min
                mean = float(np.mean(y_vals))
                eps = 1e-9

                # Two complementary signals:
                # 1) closeness: all bars are high relative to each other (high baseline)
                # 2) rel_spread: spread is modest relative to the overall level
                closeness = y_min / max(y_max, eps)                 # 0..1
                rel_spread_mean = spread / max(abs(mean), eps)      # relative to mean
                rel_spread_max = spread / max(y_max, eps)           # relative to max

                # Broader trigger: catches "tropical nights" and similar indices.
                # - closeness >= 0.80 means min is at least 80% of max
                # - rel_spread_mean <= 0.12 OR rel_spread_max <= 0.15 keeps zoom for modest spreads
                zoomed = (
                    (y_min >= 0)
                    and (closeness >= 0.80)
                    and ((rel_spread_mean <= 0.12) or (rel_spread_max <= 0.15))
                )

        if y_vals.size > 0:
            y_min = float(np.min(y_vals))
            y_max = float(np.max(y_vals))
            y_range = (y_max - y_min) if (y_max != y_min) else max(abs(y_max), 1.0)

            if zoomed:
                # Tight window around values + padding so labels/ticks have room
                pad = max(0.18 * y_range, 0.02 * max(abs(y_max), 1.0))
                ax.set_ylim(y_min - pad, y_max + pad)

                # Cue so readers know the axis is zoomed
                ax.annotate(
                    "Y-axis zoomed to highlight differences",
                    xy=(0.01, 0.98),
                    xycoords="axes fraction",
                    ha="left",
                    va="top",
                    fontsize=max(8, font_size_ticks - 1),
                    color=(0.0, 0.0, 0.0, 0.60),
                )
            else:
                # Standard view: anchor at 0 for positive-only values + headroom
                if y_min >= 0:
                    ax.set_ylim(0.0, y_max + 0.12 * y_range)
                else:
                    pad = max(0.12 * y_range, 0.02 * max(abs(y_max), 1.0))
                    ax.set_ylim(y_min - pad, y_max + pad)

        y_bottom, y_top = ax.get_ylim()
        y_span = y_top - y_bottom
        label_offset = 0.02 * y_span

        # Value labels:
        # - Zoomed mode: labels above bars (since we have a tight y-window)
        # - Normal mode: labels inside if too close to the top, else above
        for b, y in zip(bars, ys):
            if pd.isna(y):
                continue

            x_text = b.get_x() + b.get_width() / 2

            if zoomed:
                y_text = y + label_offset
                if y_text > (y_top - 0.01 * y_span):
                    y_text = y - label_offset
                    va = "top"
                else:
                    va = "bottom"

                ax.text(
                    x_text,
                    y_text,
                    format_value(y, units=units),
                    ha="center",
                    va=va,
                    fontsize=font_size_ticks,
                    color="black",
                    clip_on=True,
                )
            else:
                headroom_thresh = y_top - 0.10 * y_span
                if y >= headroom_thresh:
                    ax.text(
                        x_text,
                        y - label_offset,
                        format_value(y, units=units),
                        ha="center",
                        va="top",
                        fontsize=font_size_ticks,
                        color="white",
                        clip_on=True,
                    )
                else:
                    ax.text(
                        x_text,
                        y + label_offset,
                        format_value(y, units=units),
                        ha="center",
                        va="bottom",
                        fontsize=font_size_ticks,
                        color="black",
                        clip_on=True,
                    )

        group_centres: list[float] = []
        group_labels: list[str] = []
        for p_idx, period in enumerate(periods_present):
            group_centres.append(p_idx * group_spacing)
            group_labels.append(period_display_label(period))

        ax.set_xticks(group_centres)
        ax.set_xticklabels(group_labels, fontsize=font_size_ticks)
        ax.set_ylabel(y_label, fontsize=font_size_label)
        ax.set_title(
            f"{district_name} · Scenario comparison ({sel_stat_norm})",
            fontsize=font_size_title,
        )

        # Legend
        handles = []
        for scen in SCENARIO_ORDER:
            scen_norm = str(scen).strip().lower()
            if not any(sc == scen_norm for (sc, _) in combos):
                continue
            handles.append(
                mpatches.Patch(
                    color=SCENARIO_COLORS_HEX.get(scen_norm, "tab:blue"),
                    label=SCENARIO_DISPLAY.get(scen_norm, scen_norm),
                )
            )
        if handles:
            if zoomed:
                # In zoomed mode, keep the plot area clean: move legend to the right
                ax.legend(
                    handles=handles,
                    fontsize=font_size_legend,
                    frameon=False,
                    ncol=1,
                    loc="center left",
                    bbox_to_anchor=(1.02, 0.5),
                )
            else:
                ax.legend(
                    handles=handles,
                    fontsize=font_size_legend,
                    frameon=False,
                    ncol=min(3, len(handles)),
                    loc="upper left",
                    bbox_to_anchor=(0.0, 1.02),
                )

        ax.grid(axis="y", linestyle="--", alpha=0.35)
        ax.set_axisbelow(True)
        strip_spines(ax)

        try:
            fig.tight_layout()
        except Exception:
            pass

        return fig, ax

def create_trend_figure_for_index(
    hist_ts: pd.DataFrame,
    scen_ts: pd.DataFrame,
    idx_label: str,
    scenario_name: str,
    ax=None,
    figsize: Optional[tuple[float, float]] = None,
    fig_dpi: int = 150,
    font_size_legend: int = 8,
    *,
    units: Optional[str] = None,
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
) -> Any:
    """Create the 'Trend over time' figure (historical + scenario + bands)."""
    import matplotlib.pyplot as plt

    created_axes = ax is None

    s = style or IRTFigureStyle()
    figsize_eff = figsize or ensure_16x9_figsize(s.panel_figsize, mode="fit_width")

    y_label = idx_label
    u = (units or "").strip()
    if u and f"({u})" not in y_label:
        y_label = f"{y_label} ({u})"

    with irt_style_context(s):
        if ax is None:
            fig_ts, ax_ts = plt.subplots(figsize=figsize_eff, dpi=fig_dpi)
        else:
            ax_ts = ax
            fig_ts = ax_ts.figure

        has_any = False

        if hist_ts is not None and not hist_ts.empty:
            xh = pd.to_numeric(hist_ts["year"], errors="coerce").to_numpy(dtype=float)
            yh = pd.to_numeric(hist_ts["mean"], errors="coerce").to_numpy(dtype=float)
            ax_ts.plot(
                xh,
                yh,
                linewidth=s.line_width,
                color=SCENARIO_COLORS_HEX["historical"],
                label=SCENARIO_DISPLAY.get("historical", "Historical"),
            )
            # Optional band
            if "p05" in hist_ts.columns and "p95" in hist_ts.columns:
                p05 = pd.to_numeric(hist_ts["p05"], errors="coerce").to_numpy(dtype=float)
                p95 = pd.to_numeric(hist_ts["p95"], errors="coerce").to_numpy(dtype=float)
                ax_ts.fill_between(
                    xh,
                    p05,
                    p95,
                    color=SCENARIO_COLORS_HEX["historical"],
                    alpha=0.18,
                    linewidth=0,
                )
            has_any = True

        scen_norm = str(scenario_name).strip().lower()
        scen_color = SCENARIO_COLORS_HEX.get(scen_norm, "#d62728")
        scen_label = SCENARIO_DISPLAY.get(scen_norm, scenario_name)

        if scen_ts is not None and not scen_ts.empty:
            xs = pd.to_numeric(scen_ts["year"], errors="coerce").to_numpy(dtype=float)
            ys = pd.to_numeric(scen_ts["mean"], errors="coerce").to_numpy(dtype=float)
            ax_ts.plot(
                xs,
                ys,
                linewidth=s.line_width,
                color=scen_color,
                label=scen_label,
            )
            if "p05" in scen_ts.columns and "p95" in scen_ts.columns:
                p05 = pd.to_numeric(scen_ts["p05"], errors="coerce").to_numpy(dtype=float)
                p95 = pd.to_numeric(scen_ts["p95"], errors="coerce").to_numpy(dtype=float)
                ax_ts.fill_between(xs, p05, p95, color=scen_color, alpha=0.18, linewidth=0)
            has_any = True

        ax_ts.set_title("Trend over time", fontsize=s.title_fontsize)
        ax_ts.set_xlabel("Year")
        ax_ts.set_ylabel(y_label)
        ax_ts.grid(True, linestyle="--", alpha=0.35)
        ax_ts.set_axisbelow(True)
        strip_spines(ax_ts)

        if has_any:
            ax_ts.legend(frameon=False, fontsize=font_size_legend, loc="upper left")

        try:
            fig_ts.tight_layout()
        except Exception:
            pass

        if created_axes and logo_path:
            try:
                add_ra_logo(fig_ts, logo_path)
            except Exception:
                pass

        return fig_ts

def make_portfolio_heatmap(
    df: pd.DataFrame,
    value_col: str = "Current value",
    *,
    normalize_per_index: bool = True,
    cmap: str = "RdYlGn_r",
    figsize: Optional[Tuple[float, float]] = None,
    fig_dpi: int = 100,
    annot_fontsize: int = 9,
    label_fontsize: int = 10,
    title_fontsize: int = 12,
    title: Optional[str] = None,
) -> Optional[Any]:
    """
    Create a heatmap showing Districts (rows) × Indices (columns).
    
    Args:
        df: DataFrame from build_portfolio_multiindex_df with columns:
            State, District, Index, Group, Current value, Baseline, Δ, %Δ, 
            Rank in state, Percentile, Risk class
        value_col: Column to use for cell values. Options:
            - "Current value": Raw metric value
            - "Percentile": Percentile within state (0-100)
            - "%Δ": Percent change from baseline
            - "Δ": Absolute change from baseline
        normalize_per_index: If True, normalize values within each index column
            for better cross-index comparison. Only applies to "Current value".
        cmap: Matplotlib colormap name
        figsize: Figure size (width, height). Auto-calculated if None.
        fig_dpi: Figure DPI
        annot_fontsize: Font size for cell annotations
        label_fontsize: Font size for axis labels
        title_fontsize: Font size for title
        title: Optional title override
    
    Returns:
        matplotlib Figure or None if data is insufficient
    """
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    
    if df is None or df.empty:
        return None
    
    # Check required columns
    required_base = {"Index", value_col}
    if not required_base.issubset(df.columns):
        return None
    if not (("District" in df.columns) or ("Block" in df.columns)):
        return None

    # Create unit label:
    # - Block portfolio: "Block, District, State"
    # - District portfolio: "District, State"
    df = df.copy()
    has_block = ("Block" in df.columns) and df["Block"].notna().any()

    if has_block:
        if "State" in df.columns and "District" in df.columns:
            df["_unit_label"] = (
                df["Block"].astype(str) + ", " + df["District"].astype(str) + ", " + df["State"].astype(str)
            )
        elif "District" in df.columns:
            df["_unit_label"] = df["Block"].astype(str) + ", " + df["District"].astype(str)
        else:
            df["_unit_label"] = df["Block"].astype(str)
    else:
        if "State" in df.columns and "District" in df.columns:
            df["_unit_label"] = df["District"].astype(str) + ", " + df["State"].astype(str)
        else:
            df["_unit_label"] = df["District"].astype(str)

    # Pivot to matrix form
    try:
        pivot = df.pivot_table(
            index="_unit_label",
            columns="Index",
            values=value_col,
            aggfunc="first",
        )
    except Exception:
        return None
    
    if pivot.empty or pivot.shape[0] < 1 or pivot.shape[1] < 1:
        return None
    
    # Normalize per index if requested (only for Current value)
    display_values = pivot.copy()
    normalized_pivot = pivot.copy()
    
    if normalize_per_index and value_col == "Current value":
        for col in normalized_pivot.columns:
            col_data = normalized_pivot[col]
            col_min = col_data.min()
            col_max = col_data.max()
            if col_max != col_min:
                normalized_pivot[col] = (col_data - col_min) / (col_max - col_min)
            else:
                normalized_pivot[col] = 0.5
    
    # Calculate figure size
    n_rows, n_cols = pivot.shape
    if figsize is None:
        width = max(6, min(14, 2 + n_cols * 1.5))
        height = max(4, min(12, 1.5 + n_rows * 0.6))
        figsize = (width, height)
    
    fig, ax = plt.subplots(figsize=figsize, dpi=fig_dpi)
    
    # Create heatmap using imshow
    data_for_color = normalized_pivot.to_numpy(dtype=float)
    
    # Handle NaN for colormap
    masked_data = np.ma.masked_invalid(data_for_color)
    
    # Choose colormap based on value_col
    if value_col in ("%Δ", "Δ"):
        # Diverging colormap centered at 0 for change values
        cmap_obj = plt.get_cmap("RdBu_r")
        vmax = np.nanmax(np.abs(data_for_color))
        vmin = -vmax
        norm = mcolors.TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)
    elif value_col == "Percentile":
        # Fixed 0-100 scale for percentile
        cmap_obj = plt.get_cmap(cmap)
        norm = mcolors.Normalize(vmin=0, vmax=100)
    else:
        # Standard normalization
        cmap_obj = plt.get_cmap(cmap)
        norm = None
    
    im = ax.imshow(masked_data, cmap=cmap_obj, aspect="auto", norm=norm)
    
    # Add colorbar
    cbar = fig.colorbar(im, ax=ax, shrink=0.8)
    cbar_label = {
        "Current value": "Value (normalized)" if normalize_per_index else "Value",
        "Percentile": "Percentile",
        "%Δ": "% Change from Baseline",
        "Δ": "Change from Baseline",
    }.get(value_col, value_col)
    cbar.set_label(cbar_label, fontsize=label_fontsize)
    
    # Set ticks and labels
    ax.set_xticks(np.arange(n_cols))
    ax.set_yticks(np.arange(n_rows))
    ax.set_xticklabels(pivot.columns, fontsize=label_fontsize, rotation=45, ha="right")
    ax.set_yticklabels(pivot.index, fontsize=label_fontsize)
    
    # Add cell annotations with actual values
    for i in range(n_rows):
        for j in range(n_cols):
            val = display_values.iloc[i, j]
            if pd.notna(val):
                # Format based on value type
                if value_col == "Percentile":
                    text = f"{val:.0f}"
                elif value_col == "%Δ":
                    text = f"{val:+.1f}%"
                elif value_col == "Δ":
                    text = f"{val:+.1f}"
                else:
                    text = f"{val:.1f}"
                
                # Choose text color based on background
                bg_val = masked_data[i, j]
                if np.ma.is_masked(bg_val) or np.isnan(bg_val):
                    text_color = "black"
                else:
                    # Use white text on dark backgrounds
                    if norm:
                        normalized_bg = norm(bg_val)
                    else:
                        data_min = np.nanmin(data_for_color)
                        data_max = np.nanmax(data_for_color)
                        if data_max != data_min:
                            normalized_bg = (bg_val - data_min) / (data_max - data_min)
                        else:
                            normalized_bg = 0.5
                    text_color = "white" if 0.3 < normalized_bg < 0.7 else "black"
                
                ax.text(j, i, text, ha="center", va="center", 
                       fontsize=annot_fontsize, color=text_color)
    
    # Title
    if title is None:
        title = f"Portfolio Comparison: {value_col}"
    ax.set_title(title, fontsize=title_fontsize, pad=12)
    
    # Grid lines between cells
    ax.set_xticks(np.arange(-0.5, n_cols, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, n_rows, 1), minor=True)
    ax.grid(which="minor", color="white", linestyle="-", linewidth=2)
    ax.tick_params(which="minor", size=0)
    
    try:
        fig.tight_layout()
    except Exception:
        pass
    
    return fig


def make_portfolio_grouped_bar(
    df: pd.DataFrame,
    value_col: str = "Current value",
    *,
    max_districts: int = 10,
    max_indices: int = 6,
    figsize: Optional[Tuple[float, float]] = None,
    fig_dpi: int = 100,
    bar_width: float = 0.8,
    label_fontsize: int = 10,
    tick_fontsize: int = 9,
    title_fontsize: int = 12,
    legend_fontsize: int = 9,
    title: Optional[str] = None,
    show_values: bool = True,
    horizontal: bool = False,
) -> Optional[Any]:
    """
    Create a grouped bar chart comparing indices across districts.
    
    Args:
        df: DataFrame from build_portfolio_multiindex_df
        value_col: Column to use for bar heights. Options:
            - "Current value": Raw metric value
            - "Percentile": Percentile within state (0-100)
            - "%Δ": Percent change from baseline
        max_districts: Maximum number of districts to show
        max_indices: Maximum number of indices to show
        figsize: Figure size. Auto-calculated if None.
        fig_dpi: Figure DPI
        bar_width: Total width of bar group (0-1)
        label_fontsize: Font size for axis labels
        tick_fontsize: Font size for tick labels
        title_fontsize: Font size for title
        legend_fontsize: Font size for legend
        title: Optional title override
        show_values: Whether to show values on bars
        horizontal: If True, create horizontal bars
    
    Returns:
        matplotlib Figure or None if data is insufficient
    """
    import matplotlib.pyplot as plt
    
    if df is None or df.empty:
        return None
    
    # Check required columns
    required_base = {"Index", value_col}
    if not required_base.issubset(df.columns):
        return None
    if not (("District" in df.columns) or ("Block" in df.columns)):
        return None

    # Create unit label
    df = df.copy()
    has_block = ("Block" in df.columns) and df["Block"].notna().any()

    if has_block:
        # Keep labels compact but unique
        if "State" in df.columns and "District" in df.columns:
            df["_unit_label"] = (
                df["Block"].astype(str) + "\n" + df["District"].astype(str) + " (" + df["State"].astype(str).str[:3] + ")"
            )
        elif "District" in df.columns:
            df["_unit_label"] = df["Block"].astype(str) + "\n" + df["District"].astype(str)
        else:
            df["_unit_label"] = df["Block"].astype(str)
    else:
        if "State" in df.columns and "District" in df.columns:
            df["_unit_label"] = df["District"].astype(str) + "\n(" + df["State"].astype(str).str[:3] + ")"
        else:
            df["_unit_label"] = df["District"].astype(str)

    # Get unique units and indices
    districts = df["_unit_label"].unique()[:max_districts]
    indices = df["Index"].unique()[:max_indices]
    
    n_districts = len(districts)
    n_indices = len(indices)
    
    if n_districts < 1 or n_indices < 1:
        return None
    
    # Calculate figure size
    if figsize is None:
        if horizontal:
            width = max(8, min(14, 4 + n_indices * 0.5))
            height = max(4, min(12, 1 + n_districts * 0.8))
        else:
            width = max(8, min(14, 2 + n_districts * 1.2))
            height = max(4, min(10, 4 + n_indices * 0.3))
        figsize = (width, height)
    
    fig, ax = plt.subplots(figsize=figsize, dpi=fig_dpi)
    
    # Color palette for indices
    from india_resilience_tool.viz.colors import stable_color_map

    colors_by_index = stable_color_map([str(i) for i in indices])
    
    # Calculate bar positions
    x = np.arange(n_districts)
    width_per_bar = bar_width / n_indices
    
    # Plot bars for each index
    for i, idx_name in enumerate(indices):
        idx_data = df[df["Index"] == idx_name]
        
        values = []
        for district in districts:
            match = idx_data[idx_data["_unit_label"] == district]
            if not match.empty:
                val = match[value_col].iloc[0]
                values.append(val if pd.notna(val) else 0)
            else:
                values.append(0)
        
        offset = (i - n_indices / 2 + 0.5) * width_per_bar
        
        if horizontal:
            bars = ax.barh(x + offset, values, height=width_per_bar * 0.9, 
                          label=idx_name, color=colors_by_index.get(str(idx_name), "#777777"), edgecolor="white", linewidth=0.5)
        else:
            bars = ax.bar(x + offset, values, width=width_per_bar * 0.9,
                         label=idx_name, color=colors_by_index.get(str(idx_name), "#777777"), edgecolor="white", linewidth=0.5)
        
        # Add value labels if requested
        if show_values:
            for bar, val in zip(bars, values):
                if val == 0 or pd.isna(val):
                    continue
                
                if value_col == "Percentile":
                    text = f"{val:.0f}"
                elif value_col == "%Δ":
                    text = f"{val:+.0f}%"
                else:
                    text = f"{val:.0f}" if abs(val) >= 10 else f"{val:.1f}"
                
                if horizontal:
                    x_pos = bar.get_width()
                    y_pos = bar.get_y() + bar.get_height() / 2
                    ha = "left" if val >= 0 else "right"
                    ax.text(x_pos, y_pos, f" {text}", ha=ha, va="center", 
                           fontsize=tick_fontsize - 1)
                else:
                    x_pos = bar.get_x() + bar.get_width() / 2
                    y_pos = bar.get_height()
                    va = "bottom" if val >= 0 else "top"
                    ax.text(x_pos, y_pos, text, ha="center", va=va,
                           fontsize=tick_fontsize - 1, rotation=90 if n_indices > 4 else 0)
    
    # Set labels and ticks
    if horizontal:
        ax.set_yticks(x)
        ax.set_yticklabels(districts, fontsize=tick_fontsize)
        ax.set_xlabel(_get_value_label(value_col), fontsize=label_fontsize)
        ax.invert_yaxis()  # Top to bottom
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(districts, fontsize=tick_fontsize, rotation=45, ha="right")
        ax.set_ylabel(_get_value_label(value_col), fontsize=label_fontsize)
    
    # Add reference line at 0 for change values
    if value_col in ("%Δ", "Δ"):
        if horizontal:
            ax.axvline(x=0, color="black", linestyle="-", linewidth=0.8, alpha=0.5)
        else:
            ax.axhline(y=0, color="black", linestyle="-", linewidth=0.8, alpha=0.5)
    
    # Title
    if title is None:
        title = f"Portfolio Comparison: {_get_value_label(value_col)}"
    ax.set_title(title, fontsize=title_fontsize, pad=12)
    
    # Legend
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(1.02, 1),
        fontsize=legend_fontsize,
        frameon=False,
        title="Index",
        title_fontsize=legend_fontsize,
    )
    
    # Grid
    if horizontal:
        ax.xaxis.grid(True, linestyle="--", alpha=0.5)
    else:
        ax.yaxis.grid(True, linestyle="--", alpha=0.5)
    
    ax.set_axisbelow(True)
    
    try:
        fig.tight_layout()
    except Exception:
        pass
    
    return fig


def _get_value_label(value_col: str) -> str:
    """Helper to get human-readable label for value column."""
    return {
        "Current value": "Value",
        "Percentile": "Percentile (within state)",
        "%Δ": "% Change from Baseline",
        "Δ": "Change from Baseline",
        "Baseline": "Baseline Value",
    }.get(value_col, value_col)


def create_trend_figure_for_index_plotly(
    hist_ts: pd.DataFrame,
    scen_ts: pd.DataFrame,
    idx_label: str,
    scenario_name: str,
    *,
    compare_period_label: Optional[str] = None,
    compare_period_mean: Optional[float] = None,
    units: Optional[str] = None,
) -> Any:
    """
    Plotly version of the 'Trend over time' figure, with a teaching hoverbox.

    Hover shows:
      - YEAR: VALUE [units]
      - Δ vs <period label> mean: +/-X [units]
      - (Optional) P05–P95 band, if present in the series
    """
    import plotly.graph_objects as go

    from india_resilience_tool.viz.formatting import format_delta, format_value
    from india_resilience_tool.viz.style import apply_irt_plotly_layout

    def _prep(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        if "year" in out.columns:
            out["year"] = pd.to_numeric(out["year"], errors="coerce")
        if "mean" in out.columns:
            out["mean"] = pd.to_numeric(out["mean"], errors="coerce")
        if "p05" in out.columns:
            out["p05"] = pd.to_numeric(out["p05"], errors="coerce")
        if "p95" in out.columns:
            out["p95"] = pd.to_numeric(out["p95"], errors="coerce")
        out = out.dropna(subset=["year", "mean"]).sort_values("year").reset_index(drop=True)
        return out

    hist = _prep(hist_ts)
    scen = _prep(scen_ts)

    fig = go.Figure()

    period_lbl = (compare_period_label or "").strip()
    has_compare = (compare_period_mean is not None) and bool(period_lbl)

    # Historical
    if not hist.empty:
        delta = (hist["mean"] - float(compare_period_mean)) if has_compare else None
        customdata = None
        if has_compare:
            customdata = delta.to_numpy().reshape(-1, 1)

        hover = "<b>%{x:.0f}</b><br>" + f"Value: %{{y}}<br>"
        if has_compare:
            hover += f"Δ vs {period_lbl} mean: %{{customdata[0]}}<extra></extra>"
        else:
            hover += "<extra></extra>"

        fig.add_trace(
            go.Scatter(
                x=hist["year"],
                y=hist["mean"],
                mode="lines",
                name=SCENARIO_DISPLAY.get("historical", "Historical"),
                line={"color": SCENARIO_COLORS_HEX["historical"], "width": 2},
                customdata=customdata,
                hovertemplate=hover,
            )
        )

        if "p05" in hist.columns and "p95" in hist.columns:
            fig.add_trace(
                go.Scatter(
                    x=hist["year"],
                    y=hist["p95"],
                    mode="lines",
                    line={"width": 0},
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=hist["year"],
                    y=hist["p05"],
                    mode="lines",
                    line={"width": 0},
                    fill="tonexty",
                    fillcolor="rgba(31,119,180,0.18)",
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

    scen_norm = str(scenario_name).strip().lower()
    scen_color = SCENARIO_COLORS_HEX.get(scen_norm, "#d62728")
    scen_label = SCENARIO_DISPLAY.get(scen_norm, scenario_name)

    if not scen.empty:
        delta = (scen["mean"] - float(compare_period_mean)) if has_compare else None
        customdata = None
        if has_compare:
            customdata = delta.to_numpy().reshape(-1, 1)

        hover = "<b>%{x:.0f}</b><br>" + "Value: %{y}<br>"
        if has_compare:
            hover += f"Δ vs {period_lbl} mean: %{{customdata[0]}}<extra></extra>"
        else:
            hover += "<extra></extra>"

        fig.add_trace(
            go.Scatter(
                x=scen["year"],
                y=scen["mean"],
                mode="lines",
                name=scen_label,
                line={"color": scen_color, "width": 2},
                customdata=customdata,
                hovertemplate=hover,
            )
        )

        if "p05" in scen.columns and "p95" in scen.columns:
            fig.add_trace(
                go.Scatter(
                    x=scen["year"],
                    y=scen["p95"],
                    mode="lines",
                    line={"width": 0},
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            # approximate rgba for scenario: use same alpha with hex conversion
            fig.add_trace(
                go.Scatter(
                    x=scen["year"],
                    y=scen["p05"],
                    mode="lines",
                    line={"width": 0},
                    fill="tonexty",
                    fillcolor="rgba(214,39,40,0.18)" if scen_norm == "ssp585" else "rgba(255,127,14,0.18)",
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

    y_label = idx_label
    u = (units or "").strip()
    if u and f"({u})" not in y_label:
        y_label = f"{y_label} ({u})"

    apply_irt_plotly_layout(fig, title="Trend over time", xaxis_title="Year", yaxis_title=y_label)

    return fig


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
SCENARIO_DISPLAY: dict[str, str] = {
    "historical": "Historical",
    "ssp245": "SSP2-4.5",
    "ssp585": "SSP5-8.5",
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
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
) -> Tuple[Optional[Any], Optional[Any]]:
    """
    Build a compact bar chart showing period-mean values for each scenario.

    Notes:
    - If `ax` is provided, the caller owns figure sizing; this function will not
      add the logo to avoid duplicating it on multi-axes pages (e.g., A4 PDFs).
    """
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    if panel_df is None or panel_df.empty:
        return None, None

    s = style or IRTFigureStyle()
    figsize_eff = figsize or ensure_16x9_figsize(s.panel_figsize, mode="fit_width")

    # Keep these canonicalisations for compatibility/future highlighting
    _ = str(sel_scenario).strip().lower()
    _ = canonical_period_label(sel_period)
    _ = str(sel_stat)

    dfp = panel_df.copy()
    dfp["period"] = dfp["period"].map(canonical_period_label)
    dfp["scenario_norm"] = dfp["scenario"].astype(str).str.strip().str.lower()

    scenario_colors = {
        "historical": "tab:blue",
        "ssp245": "gold",
        "ssp585": "tab:red",
    }

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
        group_center = p_idx * group_spacing
        for i, scen_norm in enumerate(scen_here):
            offset = (i - (n_scen - 1) / 2.0) * within_spacing
            x_positions[(scen_norm, period)] = group_center + offset

    xs: list[float] = []
    ys: list[float] = []
    colors: list[str] = []

    for scen_norm, period in combos:
        mask = (dfp["scenario_norm"] == scen_norm) & (dfp["period"] == period)
        if not mask.any():
            continue
        try:
            val = float(dfp.loc[mask, "value"].iloc[0])
        except Exception:
            continue
        x_val = x_positions.get((scen_norm, period))
        if x_val is None:
            continue
        xs.append(x_val)
        ys.append(val)
        colors.append(scenario_colors.get(scen_norm, "grey"))

    if not xs:
        return None, None

    with irt_style_context(s):
        if ax is None:
            fig, ax = plt.subplots(figsize=figsize_eff, dpi=fig_dpi)
        else:
            fig = ax.figure

        ax.bar(
            xs,
            ys,
            color=colors,
            edgecolor="black",
            linewidth=0.9,
            width=0.45,
        )

        group_centres: list[float] = []
        group_labels: list[str] = []
        for p_idx, period in enumerate(periods_present):
            group_centres.append(p_idx * group_spacing)
            group_labels.append(period_display_label(period))

        ax.set_xticks(group_centres)
        ax.set_xticklabels(group_labels, fontsize=font_size_ticks)
        ax.set_ylabel(metric_label, fontsize=font_size_label)

        ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
        ax.tick_params(axis="y", labelsize=font_size_ticks)
        ax.tick_params(axis="x", labelsize=font_size_ticks)

        for x_val, y_val in zip(xs, ys):
            if y_val is None or (isinstance(y_val, float) and not np.isfinite(y_val)):
                continue
            ax.text(
                x_val,
                y_val,
                f"{y_val:.1f}",
                ha="center",
                va="bottom",
                fontsize=font_size_ticks,
            )

        ax.set_title(
            f"Scenario comparison – {district_name}",
            fontsize=font_size_title,
            pad=6,
        )

        legend_handles: list[Any] = []
        legend_labels: list[str] = []
        scen_seen = {sc for (sc, _) in combos}
        for scen in SCENARIO_ORDER:
            scen_norm = str(scen).strip().lower()
            if scen_norm in scen_seen:
                legend_handles.append(
                    mpatches.Patch(color=scenario_colors.get(scen_norm, "grey"))
                )
                legend_labels.append(SCENARIO_DISPLAY.get(scen_norm, scen_norm))

        if legend_handles:
            ax.legend(
                legend_handles,
                legend_labels,
                frameon=False,
                fontsize=font_size_legend,
                ncol=len(legend_handles),
                loc="upper left",
                bbox_to_anchor=(0.0, 1.02),
            )

        strip_spines(ax)
        try:
            fig.tight_layout()
        except Exception:
            pass

        if ax is None and logo_path:
            add_ra_logo(fig, logo_path)

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
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
) -> Any:
    """Create the 'Trend over time' figure (historical + scenario + bands).

    Notes:
    - If `ax` is provided, the caller owns figure sizing; this function will not
      add the logo to avoid duplicating it on multi-axes pages (e.g., A4 PDFs).
    """
    import matplotlib.pyplot as plt

    s = style or IRTFigureStyle()
    figsize_eff = figsize or ensure_16x9_figsize(s.panel_figsize, mode="fit_width")

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
                color="tab:blue",
                label="Historical",
            )
            if {"p05", "p95"}.issubset(hist_ts.columns):
                y05 = pd.to_numeric(hist_ts["p05"], errors="coerce").to_numpy(dtype=float)
                y95 = pd.to_numeric(hist_ts["p95"], errors="coerce").to_numpy(dtype=float)
                ax_ts.fill_between(
                    xh,
                    y05,
                    y95,
                    alpha=0.20,
                    color="tab:blue",
                )
            has_any = True

        if scen_ts is not None and not scen_ts.empty:
            scen_label = (scenario_name or "scenario").upper()
            xs = pd.to_numeric(scen_ts["year"], errors="coerce").to_numpy(dtype=float)
            ys = pd.to_numeric(scen_ts["mean"], errors="coerce").to_numpy(dtype=float)
            ax_ts.plot(
                xs,
                ys,
                linewidth=s.line_width,
                color="tab:red",
                label=scen_label,
            )
            if {"p05", "p95"}.issubset(scen_ts.columns):
                y05 = pd.to_numeric(scen_ts["p05"], errors="coerce").to_numpy(dtype=float)
                y95 = pd.to_numeric(scen_ts["p95"], errors="coerce").to_numpy(dtype=float)
                ax_ts.fill_between(
                    xs,
                    y05,
                    y95,
                    alpha=0.20,
                    color="tab:red",
                )
            has_any = True

        if (
            hist_ts is not None
            and scen_ts is not None
            and not hist_ts.empty
            and not scen_ts.empty
        ):
            try:
                last_hist_year = int(hist_ts["year"].max())
                last_hist = hist_ts.loc[hist_ts["year"] == last_hist_year].iloc[-1]

                target_year = 2020
                if "year" in scen_ts.columns and target_year in scen_ts["year"].values:
                    first_scen = scen_ts.loc[scen_ts["year"] == target_year].iloc[0]
                else:
                    first_scen = scen_ts.loc[scen_ts["year"].idxmin()]

                ax_ts.plot(
                    [float(last_hist["year"]), float(first_scen["year"])],
                    [float(last_hist["mean"]), float(first_scen["mean"])],
                    color="grey",
                    linestyle="--",
                    linewidth=1.5,
                )
            except Exception:
                pass

        ax_ts.set_xlabel("Year")
        ax_ts.set_ylabel(idx_label)

        if has_any:
            ax_ts.grid(True, linestyle=s.grid_linestyle, alpha=s.grid_alpha, linewidth=s.grid_linewidth)
            strip_spines(ax_ts)
            handles, _labels = ax_ts.get_legend_handles_labels()
            if handles:
                ax_ts.legend(frameon=False, fontsize=font_size_legend, ncol=3)

        if ax is None:
            try:
                fig_ts.tight_layout()
            except Exception:
                pass
            if logo_path:
                add_ra_logo(fig_ts, logo_path)

        return fig_ts


# =============================================================================
# Portfolio Comparison Visualizations
# =============================================================================

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
    colors = plt.cm.Set2(np.linspace(0, 1, n_indices))
    
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
                          label=idx_name, color=colors[i], edgecolor="white", linewidth=0.5)
        else:
            bars = ax.bar(x + offset, values, width=width_per_bar * 0.9,
                         label=idx_name, color=colors[i], edgecolor="white", linewidth=0.5)
        
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

    Notes:
    - This is intended for the Streamlit dashboard (interactive hover).
    - The existing Matplotlib version remains the source of truth for PDF/export.
    """
    import numpy as np
    import plotly.graph_objects as go

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

    def _units_suffix(u: Optional[str]) -> str:
        u = (u or "").strip()
        return f" {u}" if u else ""

    hist = _prep(hist_ts)
    scen = _prep(scen_ts)

    fig = go.Figure()

    # Common hover template parts
    u_suffix = _units_suffix(units)
    period_lbl = (compare_period_label or "").strip()
    has_compare = (compare_period_mean is not None) and bool(period_lbl)

    # We’ll pass customdata columns:
    #  [0] delta_vs_period_mean
    #  [1] p05 (optional, NaN if not present)
    #  [2] p95 (optional, NaN if not present)
    def _make_customdata(df: pd.DataFrame) -> np.ndarray:
        if df is None or df.empty:
            return np.empty((0, 3), dtype=float)

        delta = np.full(len(df), np.nan, dtype=float)
        if compare_period_mean is not None:
            delta = df["mean"].to_numpy(dtype=float) - float(compare_period_mean)

        p05 = df["p05"].to_numpy(dtype=float) if "p05" in df.columns else np.full(len(df), np.nan)
        p95 = df["p95"].to_numpy(dtype=float) if "p95" in df.columns else np.full(len(df), np.nan)

        return np.column_stack([delta, p05, p95])

    def _hovertemplate() -> str:
        # year + value
        ht = (
            "<b>%{x:.0f}</b>: %{y:.2f}" + u_suffix + "<br>"
        )
        # delta vs period mean (teaching)
        if has_compare:
            ht += (
                f"Δ vs {period_lbl} mean: "
                "%{customdata[0]:+.2f}" + u_suffix + "<br>"
            )
        # band (if present on that row)
        # We include it unconditionally, but it will show "nan" if missing;
        # so we use Plotly's %{customdata[i]} formatting and rely on NaNs being hidden
        # by a simple conditional string: Plotly doesn't support per-point conditionals
        # in hovertemplate, so we just show it if columns exist at least.
        ht += "<extra></extra>"
        return ht

    # Historical mean line
    if not hist.empty:
        fig.add_trace(
            go.Scatter(
                x=hist["year"],
                y=hist["mean"],
                mode="lines+markers",
                name="Historical",
                customdata=_make_customdata(hist),
                hovertemplate=_hovertemplate(),
            )
        )

        # Optional band
        if {"p05", "p95"}.issubset(hist.columns):
            fig.add_trace(
                go.Scatter(
                    x=pd.concat([hist["year"], hist["year"][::-1]]),
                    y=pd.concat([hist["p95"], hist["p05"][::-1]]),
                    fill="toself",
                    line=dict(width=0),
                    name="Historical P05–P95",
                    showlegend=False,
                    hoverinfo="skip",
                    opacity=0.15,
                )
            )

    # Scenario mean line
    if not scen.empty:
        scen_label = (scenario_name or "scenario").upper()
        fig.add_trace(
            go.Scatter(
                x=scen["year"],
                y=scen["mean"],
                mode="lines+markers",
                name=scen_label,
                customdata=_make_customdata(scen),
                hovertemplate=_hovertemplate(),
            )
        )

        # Optional band
        if {"p05", "p95"}.issubset(scen.columns):
            fig.add_trace(
                go.Scatter(
                    x=pd.concat([scen["year"], scen["year"][::-1]]),
                    y=pd.concat([scen["p95"], scen["p05"][::-1]]),
                    fill="toself",
                    line=dict(width=0),
                    name=f"{scen_label} P05–P95",
                    showlegend=False,
                    hoverinfo="skip",
                    opacity=0.15,
                )
            )

    fig.update_layout(
        title=idx_label,
        xaxis_title="Year",
        yaxis_title=(f"Value{u_suffix}" if u_suffix else "Value"),
        hovermode="x",
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig
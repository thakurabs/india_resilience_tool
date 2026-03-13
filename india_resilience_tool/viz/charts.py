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

SCENARIO_ORDER = ["historical", "ssp245", "ssp585", "bau", "opt", "pes"]

SCENARIO_DISPLAY = {
    "historical": "Historical",
    "ssp245": "SSP2-4.5",
    "ssp585": "SSP5-8.5",
    "bau": "Business-as-usual",
    "opt": "Optimistic",
    "pes": "Pessimistic",
}

# Consistent scenario colors for Plotly + Matplotlib (module-level so it's always defined)
SCENARIO_COLORS_HEX: dict[str, str] = {
    "historical": "#1f77b4",  # blue
    "ssp245": "#ff7f0e",      # orange
    "ssp585": "#d62728",      # red
    "bau": "#e66101",         # dark orange
    "opt": "#1a9850",         # green
    "pes": "#b2182b",         # dark red
}

# Risk-class palette (used for percentile-based heatmaps).
#
# These labels must remain aligned with analysis.metrics.risk_class_from_percentile:
#   <20: Very Low, >=20: Low, >=40: Medium, >=60: High, >=80: Very High
RISK_CLASS_LABELS: list[str] = ["Very Low", "Low", "Medium", "High", "Very High"]
RISK_CLASS_COLORS: list[str] = ["#1a9850", "#91cf60", "#ffffbf", "#fc8d59", "#d73027"]


def _risk_class_cmap_norm() -> tuple[Any, Any]:
    """Return (cmap, norm) for 5 risk classes, with codes 0..4."""
    import matplotlib.colors as mcolors

    cmap = mcolors.ListedColormap(RISK_CLASS_COLORS, name="irt_risk_5")
    norm = mcolors.BoundaryNorm(
        boundaries=[-0.5, 0.5, 1.5, 2.5, 3.5, 4.5],
        ncolors=len(RISK_CLASS_COLORS),
    )
    return cmap, norm


def _percentile_to_risk_code(arr: np.ndarray) -> np.ndarray:
    """
    Map percentile values to risk class codes in {0..4}, preserving NaNs.

    Codes:
      0 Very Low, 1 Low, 2 Medium, 3 High, 4 Very High
    """
    out = np.full(arr.shape, np.nan, dtype=float)
    mask = np.isfinite(arr)
    if not np.any(mask):
        return out

    a = arr[mask]
    code = np.zeros(a.shape, dtype=float)
    code = np.where(a >= 80.0, 4.0, code)
    code = np.where((a >= 60.0) & (a < 80.0), 3.0, code)
    code = np.where((a >= 40.0) & (a < 60.0), 2.0, code)
    code = np.where((a >= 20.0) & (a < 40.0), 1.0, code)
    code = np.where(a < 20.0, 0.0, code)
    out[mask] = code
    return out


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert a hex color (#RRGGBB) to a Plotly-friendly rgba(r,g,b,a) string."""
    s = str(hex_color or "").strip()
    if not s:
        return f"rgba(0,0,0,{float(alpha):.3f})"
    if s.startswith("#"):
        s = s[1:]
    if len(s) != 6:
        return f"rgba(0,0,0,{float(alpha):.3f})"
    try:
        r = int(s[0:2], 16)
        g = int(s[2:4], 16)
        b = int(s[4:6], 16)
    except Exception:
        return f"rgba(0,0,0,{float(alpha):.3f})"
    a = max(0.0, min(1.0, float(alpha)))
    return f"rgba({r},{g},{b},{a:.3f})"

PERIOD_ORDER = [
    "1979-2019",
    "1990-2010",
    "2020-2040",
    "2040-2060",
    "2060-2080",
    "2030",
    "2050",
    "2080",
]

# Human-friendly labels used in UI and chart axes (keys must match PERIOD_ORDER)
PERIOD_DISPLAY: dict[str, str] = {
    "1979-2019": "Baseline (1979-2019)",
    "1990-2010": "1990–2010",
    "2020-2040": "Early century (2020–2040)",
    "2040-2060": "Mid-century (2040–2060)",
    "2060-2080": "End century (2060–2080)",
    "2030": "2030",
    "2050": "2050",
    "2080": "2080",
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


def ordered_period_keys(periods: Sequence[str]) -> list[str]:
    """Return deduplicated period keys in canonical display order."""
    found = {canonical_period_label(p) for p in periods if str(p).strip()}
    ordered = [p for p in PERIOD_ORDER if p in found]
    ordered.extend(sorted(p for p in found if p not in PERIOD_ORDER))
    return ordered


def ordered_scenario_keys(scenarios: Sequence[str]) -> list[str]:
    """Return deduplicated scenario keys in canonical display order."""
    found = {str(s).strip().lower() for s in scenarios if str(s).strip()}
    ordered = [s for s in SCENARIO_ORDER if s in found]
    ordered.extend(sorted(s for s in found if s not in SCENARIO_ORDER))
    return ordered


def compute_scenario_y_range(
    values: Sequence[float],
    *,
    y_axis_policy: str = "auto",
) -> tuple[bool, Optional[tuple[float, float]]]:
    """
    Compute a deterministic y-axis range for the scenario comparison bar chart.

    The goal is to keep the chart readable across a wide range of metrics:
    - Many indices are nonnegative and benefit from a zero baseline when the
      range is large relative to the baseline.
    - Some indices (e.g., temperature, counts on a high baseline) have small
      relative differences that become hard to see on a strict zero baseline.
    - Some indices (e.g., SPI) can be negative; for these we always use a
      tight range around the data.

    Args:
        values: Sequence of y-values actually plotted (NaNs should be removed).
        y_axis_policy:
            - "auto" (default): zero-based unless a zoomed axis materially
              improves readability for nonnegative values.
            - "zero": force a zero baseline for nonnegative data.
            - "tight": always zoom to the data range (+padding) for nonnegative
              data; negative values always use tight.

    Returns:
        (zoomed, y_range):
          - zoomed is True when the axis does not start at 0 for nonnegative
            data (i.e., we truncated the baseline).
          - y_range is (low, high) or None if values is empty.
    """
    policy = str(y_axis_policy or "auto").strip().lower()
    if policy not in {"auto", "zero", "tight"}:
        policy = "auto"

    y_vals = np.array([float(v) for v in values if pd.notna(v)], dtype=float)
    if y_vals.size == 0:
        return False, None

    y_min = float(np.min(y_vals))
    y_max = float(np.max(y_vals))
    eps = 1e-9

    spread = y_max - y_min
    if abs(spread) <= eps:
        spread = max(abs(y_max), 1.0)

    # Any negative value: do not force a zero baseline (SPI, deltas, etc.)
    if y_min < 0:
        pad = max(0.12 * spread, 0.02 * max(abs(y_max), 1.0))
        return False, (y_min - pad, y_max + pad)

    # Nonnegative values from here on.
    if policy == "zero":
        pad = max(0.12 * spread, 0.02 * max(abs(y_max), 1.0))
        return False, (0.0, y_max + pad)

    # Decide whether to zoom (truncate baseline).
    zoom = policy == "tight"
    if policy == "auto" and not zoom:
        if y_max > 0:
            closeness = y_min / max(y_max, eps)  # 0..1
            mean = float(np.mean(y_vals))
            rel_spread_mean = (y_max - y_min) / max(abs(mean), eps)
            rel_spread_max = (y_max - y_min) / max(y_max, eps)

            close_to_max = (closeness >= 0.80) and (
                (rel_spread_mean <= 0.12) or (rel_spread_max <= 0.15)
            )

            # High-baseline metrics: baseline dominates spread (Image #2-like).
            # Example: [115..173] spread ~58, min/spread ~1.96 -> zoom.
            high_baseline = (y_min / max((y_max - y_min), eps)) >= 1.2

            zoom = close_to_max or high_baseline

    if zoom:
        pad = max(0.18 * spread, 0.02 * max(abs(y_max), 1.0))
        low = max(0.0, y_min - pad)
        high = y_max + pad
        zoomed = low > 0.0
        return zoomed, (low, high)

    pad = max(0.12 * spread, 0.02 * max(abs(y_max), 1.0))
    return False, (0.0, y_max + pad)


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
    dfp["period"] = pd.Categorical(dfp["period"], ordered_period_keys(dfp["period"].tolist()), ordered=True)
    dfp["scenario"] = pd.Categorical(dfp["scenario"], ordered_scenario_keys(dfp["scenario"].tolist()), ordered=True)
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
    y_axis_policy: str = "auto",
    units: Optional[str] = None,
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
    render_context: str = "dashboard",
) -> Tuple[Optional[Any], Optional[Any]]:
    """
    Build a compact bar chart showing period-mean values for each scenario.

    Args:
        panel_df: DataFrame with columns at least [scenario, period, value].
        metric_label: Human-friendly metric name for axis labeling.
        sel_scenario: Selected scenario key (e.g. "ssp245") for highlighting.
        sel_period: Selected period (e.g. "2020-2040") for highlighting.
        sel_stat: Selected statistic (e.g. "mean").
        district_name: District name (used only for dashboard titles).
        ax: Optional Matplotlib axes to draw into.
        render_context: "dashboard" (default) or "pdf". In PDF context we avoid
            redundant titles/annotations and use compact axis labels.

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

    ctx = str(render_context).strip().lower()
    is_pdf = ctx in {"pdf", "report", "export"}

    s = style or IRTFigureStyle()
    figsize_eff = figsize or ensure_16x9_figsize(s.panel_figsize, mode="fit_width")

    # Canonicalise inputs for highlighting
    sel_scenario_norm = str(sel_scenario).strip().lower()
    sel_period_norm = canonical_period_label(sel_period)
    sel_stat_norm = str(sel_stat)

    dfp = panel_df.copy()
    dfp["period"] = dfp["period"].map(canonical_period_label)
    dfp["scenario_norm"] = dfp["scenario"].astype(str).str.strip().str.lower()

    scenario_order = ordered_scenario_keys(dfp["scenario_norm"].tolist())
    period_order = ordered_period_keys(dfp["period"].tolist())

    combos: list[tuple[str, str]] = []
    for scen in scenario_order:
        scen_norm = str(scen).strip().lower()
        for period in period_order:
            mask = (dfp["scenario_norm"] == scen_norm) & (dfp["period"] == period)
            if mask.any():
                combos.append((scen_norm, period))

    if not combos:
        return None, None

    periods_present: list[str] = []
    for period in period_order:
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

        # Outlines on all bars; selected bar gets a stronger outline.
        edgecolors.append((0.0, 0.0, 0.0, 0.75) if is_selected else (0.0, 0.0, 0.0, 0.55))
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

        y_range_res = compute_scenario_y_range(
            [v for v in ys if pd.notna(v)],
            y_axis_policy=y_axis_policy,
        )
        zoomed, y_range = y_range_res
        if y_range is not None:
            ax.set_ylim(*y_range)

        y_bottom, y_top = ax.get_ylim()
        y_span = y_top - y_bottom
        label_offset = 0.02 * y_span

        for b, y_val in zip(bars, ys):
            if pd.isna(y_val):
                continue

            x_text = b.get_x() + b.get_width() / 2

            if zoomed:
                y_text = y_val + label_offset
                if y_text > (y_top - 0.01 * y_span):
                    y_text = y_val - label_offset
                    va = "top"
                else:
                    va = "bottom"

                ax.text(
                    x_text,
                    y_text,
                    format_value(y_val, units=units),
                    ha="center",
                    va=va,
                    fontsize=font_size_ticks,
                    color="black",
                    clip_on=True,
                )
            else:
                headroom_thresh = y_top - 0.10 * y_span
                if y_val >= headroom_thresh:
                    ax.text(
                        x_text,
                        y_val - label_offset,
                        format_value(y_val, units=units),
                        ha="center",
                        va="top",
                        fontsize=font_size_ticks,
                        color="white",
                        clip_on=True,
                    )
                else:
                    ax.text(
                        x_text,
                        y_val + label_offset,
                        format_value(y_val, units=units),
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
            if is_pdf:
                group_labels.append(str(period).replace("-", "–"))
            else:
                group_labels.append(period_display_label(period))

        ax.set_xticks(group_centres)
        ax.set_xticklabels(group_labels, fontsize=font_size_ticks)
        ax.set_ylabel(y_label, fontsize=font_size_label)

        if not is_pdf:
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
            if is_pdf:
                ax.legend(
                    handles=handles,
                    fontsize=font_size_legend,
                    frameon=False,
                    ncol=1,
                    loc="upper right",
                )
            else:
                if zoomed:
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

        if is_pdf and zoomed:
            # PDF exports have no UI toggle; add a subtle disclosure that the
            # y-axis is zoomed (does not start at zero).
            try:
                ax.text(
                    0.99,
                    0.01,
                    "y-axis zoomed",
                    transform=ax.transAxes,
                    ha="right",
                    va="bottom",
                    fontsize=max(6, font_size_ticks - 2),
                    color="0.4",
                )
            except Exception:
                pass

        if is_pdf:
            # Publication-style: keep left/bottom spines; hide top/right.
            try:
                ax.spines["top"].set_visible(False)
                ax.spines["right"].set_visible(False)
            except Exception:
                pass
        else:
            strip_spines(ax)

        # Only run tight_layout when we created the figure; otherwise it can
        # fight the caller's gridspec/subplot layout (notably in PDFs).
        if created_axes:
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
    render_context: str = "dashboard",
) -> Any:
    """Create the 'Trend over time' figure (historical + scenario + bands)."""
    import matplotlib.pyplot as plt

    created_axes = ax is None

    def _value_column(df: pd.DataFrame) -> str:
        """
        Prefer a robust central tendency for the trend line.

        Many indices (e.g., spell durations) can produce occasional
        pathological values in a single model/year (e.g., an entire year
        flagged as a cold spell). Using the ensemble median for the trend
        line keeps historical and scenario segments contiguous while still
        showing variability via p05–p95 shading. Fall back to mean when a
        median is unavailable.
        """
        for cand in ("median", "ensemble_median", "mean", "ensemble_mean"):
            if cand in df.columns:
                if pd.to_numeric(df[cand], errors="coerce").notna().any():
                    return cand
        return "mean"

    ctx = str(render_context).strip().lower()
    is_pdf = ctx in {"pdf", "report", "export"}

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
            hist_val_col = _value_column(hist_ts)
            yh = pd.to_numeric(hist_ts[hist_val_col], errors="coerce").to_numpy(dtype=float)
            ax_ts.plot(
                xh,
                yh,
                linewidth=s.line_width,
                color=SCENARIO_COLORS_HEX["historical"],
                label=SCENARIO_DISPLAY.get("historical", "Historical"),
            )
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
            scen_val_col = _value_column(scen_ts)
            ys = pd.to_numeric(scen_ts[scen_val_col], errors="coerce").to_numpy(dtype=float)
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

        if not is_pdf:
            ax_ts.set_title("Trend over time", fontsize=s.title_fontsize)

        ax_ts.set_xlabel("Year")
        ax_ts.set_ylabel(y_label)
        ax_ts.grid(True, linestyle="--", alpha=0.35)
        ax_ts.set_axisbelow(True)

        if is_pdf:
            try:
                ax_ts.spines["top"].set_visible(False)
                ax_ts.spines["right"].set_visible(False)
            except Exception:
                pass
        else:
            strip_spines(ax_ts)

        if has_any:
            ax_ts.legend(frameon=False, fontsize=font_size_legend, loc="upper left")

        # Only run tight_layout when we created the figure; otherwise it can
        # fight the caller's gridspec/subplot layout (notably in PDFs).
        if created_axes:
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
        # Risk-class palette for percentile (colors show category; numbers show percentiles).
        cmap_obj, norm = _risk_class_cmap_norm()
        data_for_color = _percentile_to_risk_code(pivot.to_numpy(dtype=float))
        masked_data = np.ma.masked_invalid(data_for_color)
    else:
        # Standard normalization
        cmap_obj = plt.get_cmap(cmap)
        norm = None
    
    im = ax.imshow(masked_data, cmap=cmap_obj, aspect="auto", norm=norm)
    
    # Add colorbar
    cbar = fig.colorbar(im, ax=ax, shrink=0.8)
    if value_col == "Percentile":
        cbar.set_ticks([0, 1, 2, 3, 4])
        cbar.set_ticklabels(RISK_CLASS_LABELS)
        try:
            cbar.ax.set_label("_portfolio_heatmap_percentile_colorbar")
        except Exception:
            pass
    else:
        cbar_label = {
            "Current value": "Value (normalized)" if normalize_per_index else "Value",
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


def make_scenario_comparison_figure_dashboard(**kwargs):
    """
    Return a Plotly scenario comparison when available; fall back to Matplotlib.

    The dashboard prefers Plotly for visual consistency. If Plotly is unavailable
    (or the Plotly builder fails), fall back to the Matplotlib version to avoid
    breaking the UI.
    """
    try:
        fig = make_scenario_comparison_figure_plotly(render_context="dashboard", **kwargs)
        if fig is not None:
            return fig
    except Exception:
        pass

    return make_scenario_comparison_figure(render_context="dashboard", **kwargs)


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


# -----------------------------------------------------------------------------
# Portfolio scenario compare helpers (used by Portfolio Compare visualizations)
# -----------------------------------------------------------------------------

def _scenario_sort_key(s: str) -> tuple[int, str]:
    s_norm = str(s or "").strip().lower()
    if s_norm in SCENARIO_ORDER:
        return (0, str(SCENARIO_ORDER.index(s_norm)))
    return (1, s_norm)


def _scenario_display(s: str) -> str:
    s_norm = str(s or "").strip().lower()
    return str(SCENARIO_DISPLAY.get(s_norm, s)).strip()


def _prep_portfolio_unit_label(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a deterministic unit label column for portfolio matrices.

    Contract:
      - Block portfolio: "Block, District, State"
      - District portfolio: "District, State"
    """
    out = df.copy()
    has_block = ("Block" in out.columns) and out["Block"].notna().any()

    if has_block:
        if {"State", "District"}.issubset(out.columns):
            out["_unit_label"] = (
                out["Block"].astype(str)
                + ", "
                + out["District"].astype(str)
                + ", "
                + out["State"].astype(str)
            )
        elif "District" in out.columns:
            out["_unit_label"] = out["Block"].astype(str) + ", " + out["District"].astype(str)
        else:
            out["_unit_label"] = out["Block"].astype(str)
    else:
        if {"State", "District"}.issubset(out.columns):
            out["_unit_label"] = out["District"].astype(str) + ", " + out["State"].astype(str)
        else:
            out["_unit_label"] = out["District"].astype(str)
    return out


def build_portfolio_pivot(
    df: pd.DataFrame,
    *,
    value_col: str,
    scenario: Optional[str] = None,
) -> pd.DataFrame:
    """
    Build a unit × index matrix for portfolio visualizations.

    If `scenario` is provided, filters rows where `Scenario == scenario` (after strip()).
    """
    if df is None or df.empty:
        return pd.DataFrame()
    if "Index" not in df.columns or value_col not in df.columns:
        return pd.DataFrame()
    if not (("District" in df.columns) or ("Block" in df.columns)):
        return pd.DataFrame()

    out = df.copy()
    if scenario is not None and "Scenario" in out.columns:
        scen = str(scenario).strip()
        out = out[out["Scenario"].astype(str).str.strip() == scen]
        if out.empty:
            return pd.DataFrame()

    out = _prep_portfolio_unit_label(out)
    try:
        pivot = out.pivot_table(
            index="_unit_label",
            columns="Index",
            values=value_col,
            aggfunc="first",
        )
    except Exception:
        return pd.DataFrame()
    return pivot


def build_portfolio_scenario_min_pivot(
    df: pd.DataFrame,
    *,
    value_col: str,
    scenarios: Sequence[str],
) -> pd.DataFrame:
    """Compute elementwise min across scenarios on the unit × index pivot (NaN-aware)."""
    if df is None or df.empty:
        return pd.DataFrame()

    scenarios_norm = [str(s).strip() for s in (scenarios or []) if str(s).strip()]
    if len(scenarios_norm) < 1:
        return pd.DataFrame()

    pivots: list[pd.DataFrame] = []
    for scen in scenarios_norm:
        pv = build_portfolio_pivot(df, value_col=value_col, scenario=scen)
        if pv is not None and not pv.empty:
            pivots.append(pv)

    if not pivots:
        return pd.DataFrame()

    idx = sorted({i for pv in pivots for i in pv.index})
    cols = sorted({c for pv in pivots for c in pv.columns}, key=lambda x: str(x))
    mats: list[np.ndarray] = []
    for pv in pivots:
        mats.append(pv.reindex(index=idx, columns=cols).to_numpy(dtype=float))

    stack = np.stack(mats, axis=0)  # (S, R, C)
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="All-NaN slice encountered", category=RuntimeWarning)
        out = np.nanmin(stack, axis=0)

    return pd.DataFrame(out, index=idx, columns=cols)


def make_portfolio_heatmap_scenario_panels(
    df: pd.DataFrame,
    *,
    value_col: str,
    scenarios: Sequence[str],
    normalize_per_index: bool = True,
    cmap: str = "RdYlGn_r",
    layout: str = "horizontal",
    hide_xticklabels_except_last: bool = False,
    hspace: float = 0.18,
    wspace: float = 0.06,
    figsize: Optional[Tuple[float, float]] = None,
    fig_dpi: int = 100,
    annot_fontsize: int = 9,
    label_fontsize: int = 10,
    title_fontsize: int = 12,
    title: Optional[str] = None,
) -> Optional[Any]:
    """
    Scenario panels heatmap: one heatmap per scenario, with a shared color scale.

    Notes:
      - Percentile uses a fixed 0–100 scale.
      - Δ/%Δ use a shared diverging scale centered at 0.
      - Current value can be normalized per index across all selected scenarios.
    """
    import matplotlib.colors as mcolors
    import matplotlib.pyplot as plt

    if df is None or df.empty:
        return None
    if not scenarios:
        return None

    scenarios_norm = [str(s).strip() for s in scenarios if str(s).strip()]
    if not scenarios_norm:
        return None

    pivots: dict[str, pd.DataFrame] = {}
    for scen in scenarios_norm:
        pv = build_portfolio_pivot(df, value_col=value_col, scenario=scen)
        if not pv.empty:
            pivots[scen] = pv

    if not pivots:
        return None

    # Align pivots to a common index/columns union for consistent axes.
    all_idx = sorted({i for pv in pivots.values() for i in pv.index})
    all_cols = sorted({c for pv in pivots.values() for c in pv.columns}, key=lambda x: str(x))
    for scen in list(pivots.keys()):
        pivots[scen] = pivots[scen].reindex(index=all_idx, columns=all_cols)

    # Prepare color data + shared norm
    if value_col in ("%Δ", "Δ"):
        cmap_obj = plt.get_cmap("RdBu_r")
        all_vals = np.concatenate([pv.to_numpy(dtype=float).ravel() for pv in pivots.values()])
        vmax = float(np.nanmax(np.abs(all_vals))) if np.isfinite(all_vals).any() else 1.0
        vmax = max(vmax, 1e-9)
        norm: Optional[Any] = mcolors.TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)
        color_mats = {k: v.to_numpy(dtype=float) for k, v in pivots.items()}
        cbar_label = {"%Δ": "%Δ (vs baseline)", "Δ": "Δ (vs baseline)"}.get(value_col, value_col)
    elif value_col == "Percentile":
        cmap_obj, norm = _risk_class_cmap_norm()
        color_mats = {k: _percentile_to_risk_code(v.to_numpy(dtype=float)) for k, v in pivots.items()}
        cbar_label = "Risk class (from percentile)"
    else:
        cmap_obj = plt.get_cmap(cmap)
        cbar_label = "Value"
        if value_col == "Current value" and normalize_per_index:
            # Normalize per index across all selected scenarios (shared scale).
            stacked = pd.concat(pivots.values(), axis=0)
            mins = stacked.min(axis=0, skipna=True)
            maxs = stacked.max(axis=0, skipna=True)

            color_mats = {}
            for scen, pv in pivots.items():
                pv_n = pv.copy()
                for col in pv_n.columns:
                    mn = float(mins.get(col)) if col in mins.index else float("nan")
                    mx = float(maxs.get(col)) if col in maxs.index else float("nan")
                    if pd.isna(mn) or pd.isna(mx) or mx == mn:
                        pv_n[col] = 0.5
                    else:
                        pv_n[col] = (pv_n[col] - mn) / (mx - mn)
                color_mats[scen] = pv_n.to_numpy(dtype=float)
            norm = mcolors.Normalize(vmin=0, vmax=1)
            cbar_label = "Value (normalized per index)"
        else:
            all_vals = np.concatenate([pv.to_numpy(dtype=float).ravel() for pv in pivots.values()])
            if all_vals.size and np.isfinite(all_vals).any():
                vmin = float(np.nanmin(all_vals))
                vmax = float(np.nanmax(all_vals))
                if vmin == vmax:
                    vmin -= 1.0
                    vmax += 1.0
                norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
            else:
                norm = None
            color_mats = {k: v.to_numpy(dtype=float) for k, v in pivots.items()}

    layout_norm = str(layout or "horizontal").strip().lower()
    n_panels = len(pivots)
    n_rows = 1 if layout_norm.startswith("h") else n_panels
    n_cols = n_panels if layout_norm.startswith("h") else 1

    # Auto figure size
    if figsize is None:
        base_w = max(6, min(14, 2 + len(all_cols) * 1.2))
        base_h = max(4, min(12, 1.5 + len(all_idx) * 0.55))
        if n_cols > 1:
            figsize = (min(18, base_w * n_cols), base_h)
        else:
            figsize = (base_w, min(18, base_h * n_rows))

    gridspec_kw = None
    if n_cols > 1:
        gridspec_kw = {"wspace": float(wspace)}
    if n_rows > 1:
        gs = dict(gridspec_kw or {})
        gs["hspace"] = float(hspace)
        gridspec_kw = gs
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=figsize,
        dpi=fig_dpi,
        squeeze=False,
        gridspec_kw=gridspec_kw,
    )

    scen_ordered = sorted(pivots.keys(), key=_scenario_sort_key)
    last_im = None
    for i, scen in enumerate(scen_ordered):
        r = 0 if n_rows == 1 else i
        c = i if n_cols > 1 else 0
        ax = axes[r][c]

        data_for_color = np.ma.masked_invalid(color_mats[scen])
        last_im = ax.imshow(data_for_color, cmap=cmap_obj, aspect="auto", norm=norm)

        ax.set_title(_scenario_display(scen), fontsize=title_fontsize, pad=10)
        ax.set_xticks(np.arange(len(all_cols)))
        ax.set_yticks(np.arange(len(all_idx)))
        ax.set_xticklabels(all_cols, fontsize=label_fontsize, rotation=45, ha="right")
        ax.set_yticklabels(all_idx, fontsize=label_fontsize)
        if n_cols > 1 and c != 0:
            # Share y-axis labels across panels to improve readability.
            ax.tick_params(axis="y", which="both", left=False, labelleft=False)

        if hide_xticklabels_except_last and (n_rows > 1) and (r != n_rows - 1):
            ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)

        # Annotate with actual values (not normalized)
        display_vals = pivots[scen]
        for rr in range(display_vals.shape[0]):
            for cc in range(display_vals.shape[1]):
                val = display_vals.iloc[rr, cc]
                if pd.isna(val):
                    continue
                if value_col == "Percentile":
                    text = f"{float(val):.0f}"
                elif value_col == "%Δ":
                    text = f"{float(val):+.1f}%"
                elif value_col == "Δ":
                    text = f"{float(val):+.1f}"
                else:
                    text = f"{float(val):.1f}"
                ax.text(cc, rr, text, ha="center", va="center", fontsize=annot_fontsize)

        ax.set_xticks(np.arange(-0.5, len(all_cols), 1), minor=True)
        ax.set_yticks(np.arange(-0.5, len(all_idx), 1), minor=True)
        ax.grid(which="minor", color="white", linestyle="-", linewidth=2)
        ax.tick_params(which="minor", size=0)

    if title is None:
        title = f"Scenario panels: {value_col}"
    fig.suptitle(title, fontsize=title_fontsize + 1, y=0.98)

    base_bottom = 0.30
    try:
        # Leave generous bottom space for rotated x-labels.
        # Use a heuristic based on the number of x labels (long metric names).
        if len(all_cols) >= 8:
            base_bottom = 0.34
        if len(all_cols) >= 12:
            base_bottom = 0.38
        if not layout_norm.startswith("h"):
            base_bottom = max(0.28, base_bottom - 0.06)
    except Exception:
        base_bottom = 0.30

    if last_im is not None:
        if value_col == "Percentile":
            # Use Matplotlib-managed sizing (like robust min risk heatmap) for a consistent look.
            cbar = fig.colorbar(last_im, ax=axes.ravel().tolist(), shrink=0.85)
            cbar.set_ticks([0, 1, 2, 3, 4])
            cbar.set_ticklabels(RISK_CLASS_LABELS)
            cbar.ax.tick_params(labelsize=label_fontsize)
            try:
                cbar.ax.set_label("_portfolio_scenario_panels_colorbar")
            except Exception:
                pass
            try:
                fig.tight_layout(rect=[0, base_bottom, 0.98, 0.95])
            except Exception:
                pass
        else:
            try:
                fig.tight_layout(rect=[0, base_bottom, 1.0, 0.95])
            except Exception:
                pass
            # Keep continuous values consistent: horizontal colorbar, centered below.
            positions = [ax.get_position() for ax in axes.ravel().tolist()]
            left = float(min(p.x0 for p in positions))
            right = float(max(p.x1 for p in positions))
            span = max(right - left, 1e-6)
            cbar_width = max(0.25, min(0.70, span * 0.70))
            cbar_left = left + (span - cbar_width) / 2.0
            cbar_height = 0.030
            cbar_bottom = 0.06

            cax = fig.add_axes([cbar_left, cbar_bottom, cbar_width, cbar_height])
            cax.set_label("_portfolio_scenario_panels_colorbar")
            cbar = fig.colorbar(last_im, cax=cax, orientation="horizontal")
            cbar.set_label(cbar_label, fontsize=label_fontsize)
    return fig


def make_portfolio_heatmap_robust_min_percentile(
    df: pd.DataFrame,
    *,
    scenarios: Sequence[str],
    figsize: Optional[Tuple[float, float]] = None,
    fig_dpi: int = 100,
    annot_fontsize: int = 9,
    label_fontsize: int = 10,
    title_fontsize: int = 12,
    title: Optional[str] = None,
) -> Optional[Any]:
    """
    Robust risk heatmap (No-regrets): min percentile across selected scenarios.

    This highlights risk signals that remain high even under the less-severe scenario.
    """
    import matplotlib.pyplot as plt

    if df is None or df.empty:
        return None

    scen_list = [str(s).strip() for s in (scenarios or []) if str(s).strip()]
    if len(scen_list) < 2:
        return None

    min_pv = build_portfolio_scenario_min_pivot(df, value_col="Percentile", scenarios=scen_list)
    if min_pv is None or min_pv.empty:
        return None

    cmap_obj, norm = _risk_class_cmap_norm()
    try:
        cmap_obj.set_bad("#cccccc")  # type: ignore[attr-defined]
    except Exception:
        pass

    codes = _percentile_to_risk_code(min_pv.to_numpy(dtype=float))
    masked = np.ma.masked_invalid(codes)

    n_rows, n_cols = min_pv.shape
    if figsize is None:
        width = max(6, min(14, 2 + n_cols * 1.5))
        height = max(4, min(12, 1.5 + n_rows * 0.6))
        figsize = (width, height)

    fig, ax = plt.subplots(figsize=figsize, dpi=fig_dpi)
    im = ax.imshow(masked, cmap=cmap_obj, aspect="auto", norm=norm)

    ax.set_xticks(np.arange(n_cols))
    ax.set_yticks(np.arange(n_rows))
    ax.set_xticklabels(min_pv.columns, fontsize=label_fontsize, rotation=45, ha="right")
    ax.set_yticklabels(min_pv.index, fontsize=label_fontsize)

    # Annotations: min percentile values
    for i in range(n_rows):
        for j in range(n_cols):
            val = min_pv.iloc[i, j]
            if pd.isna(val):
                continue
            ax.text(j, i, f"{float(val):.0f}", ha="center", va="center", fontsize=annot_fontsize)

    # Grid lines
    ax.set_xticks(np.arange(-0.5, n_cols, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, n_rows, 1), minor=True)
    ax.grid(which="minor", color="white", linestyle="-", linewidth=2)
    ax.tick_params(which="minor", size=0)

    scen_disp = ", ".join([_scenario_display(s) for s in scen_list])
    if title is None:
        title = f"Robust risk (min percentile) • {scen_disp}"
    ax.set_title(title, fontsize=title_fontsize, pad=12)

    # Vertical right colorbar (5 bins)
    cbar = fig.colorbar(im, ax=ax, shrink=0.85)
    cbar.set_ticks([0, 1, 2, 3, 4])
    cbar.set_ticklabels(RISK_CLASS_LABELS)
    cbar.ax.tick_params(labelsize=label_fontsize)

    # Mark the colorbar axes for tests (findable without pixel asserts).
    try:
        cbar.ax.set_label("_portfolio_robust_min_colorbar")
    except Exception:
        pass

    try:
        fig.tight_layout()
    except Exception:
        pass
    return fig


def make_portfolio_scenario_grouped_bar(
    df: pd.DataFrame,
    *,
    index_name: str,
    value_col: str,
    scenarios: Sequence[str],
    sort_mode: str = "scenario_b_desc",
    horizontal: bool = True,
    show_values: bool = True,
    max_units: int = 15,
    figsize: Optional[Tuple[float, float]] = None,
    fig_dpi: int = 100,
    label_fontsize: int = 10,
    tick_fontsize: int = 9,
    title_fontsize: int = 12,
    legend_fontsize: int = 9,
    title: Optional[str] = None,
) -> Optional[Any]:
    """Grouped bars per unit, colored by scenario, for a single portfolio index."""
    import matplotlib.pyplot as plt

    if df is None or df.empty:
        return None
    if "Scenario" not in df.columns or "Index" not in df.columns or value_col not in df.columns:
        return None
    if not (("District" in df.columns) or ("Block" in df.columns)):
        return None

    scenarios_norm = [str(s).strip() for s in scenarios if str(s).strip()]
    if not scenarios_norm:
        return None

    dfi = df[df["Index"].astype(str) == str(index_name)]
    if dfi.empty:
        return None

    dfi = dfi[dfi["Scenario"].astype(str).str.strip().isin(set(scenarios_norm))]
    if dfi.empty:
        return None

    # Create compact unit labels (same style as existing grouped bar).
    dfi = dfi.copy()
    has_block = ("Block" in dfi.columns) and dfi["Block"].notna().any()
    if has_block:
        if "State" in dfi.columns and "District" in dfi.columns:
            dfi["_unit_label"] = (
                dfi["Block"].astype(str)
                + "\n"
                + dfi["District"].astype(str)
                + " ("
                + dfi["State"].astype(str).str[:3]
                + ")"
            )
        elif "District" in dfi.columns:
            dfi["_unit_label"] = dfi["Block"].astype(str) + "\n" + dfi["District"].astype(str)
        else:
            dfi["_unit_label"] = dfi["Block"].astype(str)
    else:
        if "State" in dfi.columns and "District" in dfi.columns:
            dfi["_unit_label"] = dfi["District"].astype(str) + "\n(" + dfi["State"].astype(str).str[:3] + ")"
        else:
            dfi["_unit_label"] = dfi["District"].astype(str)

    # Determine unit ordering
    units = list(pd.unique(dfi["_unit_label"]))[:max_units]
    if sort_mode == "scenario_b_desc" and len(scenarios_norm) >= 2:
        scen_b = scenarios_norm[-1]
        key_vals = []
        for u in units:
            sub = dfi[(dfi["_unit_label"] == u) & (dfi["Scenario"].astype(str).str.strip() == scen_b)]
            if sub.empty:
                key_vals.append(float("-inf"))
            else:
                v = pd.to_numeric(sub[value_col].iloc[0], errors="coerce")
                key_vals.append(float(v) if pd.notna(v) else float("-inf"))
        units = [u for _, u in sorted(zip(key_vals, units), reverse=True)]

    n_units = len(units)
    n_scens = len(scenarios_norm)
    if n_units < 1 or n_scens < 1:
        return None

    if figsize is None:
        if horizontal:
            width = max(8, min(14, 5 + n_scens * 1.2))
            height = max(4, min(12, 1 + n_units * 0.7))
        else:
            width = max(8, min(14, 2 + n_units * 1.1))
            height = max(4, min(10, 4 + n_scens * 0.3))
        figsize = (width, height)

    fig, ax = plt.subplots(figsize=figsize, dpi=fig_dpi)

    # Scenario colors (fallback to stable mapping for unknowns)
    from india_resilience_tool.viz.colors import stable_color_map

    fallback_colors = stable_color_map([str(s) for s in scenarios_norm])
    colors_by_scen = {}
    for s in scenarios_norm:
        s_norm = str(s).strip().lower()
        colors_by_scen[s] = SCENARIO_COLORS_HEX.get(s_norm, fallback_colors.get(str(s), "#777777"))

    x = np.arange(n_units)
    group_width = 0.8
    width_per = group_width / max(1, n_scens)

    for i, scen in enumerate(scenarios_norm):
        vals: list[float] = []
        missing_mask: list[bool] = []
        for u in units:
            sub = dfi[(dfi["_unit_label"] == u) & (dfi["Scenario"].astype(str).str.strip() == scen)]
            if sub.empty:
                vals.append(float("nan"))
                missing_mask.append(True)
            else:
                v = pd.to_numeric(sub[value_col].iloc[0], errors="coerce")
                vals.append(float(v) if pd.notna(v) else float("nan"))
                missing_mask.append(pd.isna(v))

        offset = (i - n_scens / 2 + 0.5) * width_per
        label = _scenario_display(scen)
        color = colors_by_scen.get(scen, "#777777")

        if horizontal:
            bars = ax.barh(
                x + offset,
                [0.0 if pd.isna(v) else v for v in vals],
                height=width_per * 0.9,
                label=label,
                color=color,
                edgecolor="white",
                linewidth=0.5,
                alpha=0.25 if all(missing_mask) else 0.95,
            )
        else:
            bars = ax.bar(
                x + offset,
                [0.0 if pd.isna(v) else v for v in vals],
                width=width_per * 0.9,
                label=label,
                color=color,
                edgecolor="white",
                linewidth=0.5,
                alpha=0.25 if all(missing_mask) else 0.95,
            )

        if show_values:
            for bar, raw in zip(bars, vals):
                if pd.isna(raw) or float(raw) == 0.0:
                    continue
                if value_col == "Percentile":
                    text = f"{float(raw):.0f}"
                elif value_col == "%Δ":
                    text = f"{float(raw):+.0f}%"
                else:
                    text = f"{float(raw):.0f}" if abs(float(raw)) >= 10 else f"{float(raw):.1f}"

                if horizontal:
                    ax.text(
                        bar.get_width(),
                        bar.get_y() + bar.get_height() / 2,
                        f" {text}",
                        ha="left",
                        va="center",
                        fontsize=tick_fontsize - 1,
                    )
                else:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bar.get_height(),
                        text,
                        ha="center",
                        va="bottom",
                        fontsize=tick_fontsize - 1,
                        rotation=90 if n_scens > 3 else 0,
                    )

    if horizontal:
        ax.set_yticks(x)
        ax.set_yticklabels(units, fontsize=tick_fontsize)
        ax.set_xlabel(_get_value_label(value_col), fontsize=label_fontsize)
        ax.invert_yaxis()
        ax.xaxis.grid(True, linestyle="--", alpha=0.5)
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(units, fontsize=tick_fontsize, rotation=45, ha="right")
        ax.set_ylabel(_get_value_label(value_col), fontsize=label_fontsize)
        ax.yaxis.grid(True, linestyle="--", alpha=0.5)

    if value_col in ("%Δ", "Δ"):
        if horizontal:
            ax.axvline(x=0, color="black", linestyle="-", linewidth=0.8, alpha=0.5)
        else:
            ax.axhline(y=0, color="black", linestyle="-", linewidth=0.8, alpha=0.5)

    if title is None:
        title = f"{index_name} • {_get_value_label(value_col)} by scenario"
    ax.set_title(title, fontsize=title_fontsize, pad=12)

    ax.legend(
        loc="upper left",
        bbox_to_anchor=(1.02, 1),
        fontsize=legend_fontsize,
        frameon=False,
        title="Scenario",
        title_fontsize=legend_fontsize,
    )
    ax.set_axisbelow(True)

    try:
        fig.tight_layout()
    except Exception:
        pass
    return fig


def make_scenario_comparison_figure_plotly(
    panel_df: pd.DataFrame,
    metric_label: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    district_name: str,
    ax: Any = None,
    figsize: Optional[tuple[float, float]] = None,
    fig_dpi: int = 150,
    font_size_title: int = 11,
    font_size_label: int = 10,
    font_size_ticks: int = 9,
    font_size_legend: int = 9,
    *,
    y_axis_policy: str = "auto",
    units: Optional[str] = None,
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
    render_context: str = "dashboard",
) -> Any:
    """Plotly version of the scenario comparison (period-mean) chart.

    This function exists primarily for the dashboard, where Plotly provides a
    consistent look-and-feel with the Plotly trend chart.

    Notes:
    - Parameters like ``ax``, ``figsize``, ``fig_dpi`` and font sizes are
      accepted for API compatibility but are not used.
    - ``logo_path`` is ignored here; PDF exports apply logos at the page level.

    Returns:
        A Plotly Figure (or ``None`` if input is empty).
    """
    if panel_df is None or panel_df.empty:
        return None

    try:
        import plotly.graph_objects as go
    except Exception:
        # Plotly not available; caller should fall back to Matplotlib.
        return None

    from india_resilience_tool.viz.style import apply_irt_plotly_layout

    ctx = str(render_context).strip().lower()
    is_pdf = ctx in {"pdf", "report", "export"}

    dfp = panel_df.copy()
    dfp["period"] = dfp["period"].map(canonical_period_label)
    dfp["scenario_norm"] = dfp["scenario"].astype(str).str.strip().str.lower()

    # Periods present in canonical order
    periods_present: list[str] = []
    for p in PERIOD_ORDER:
        if (dfp["period"] == p).any():
            periods_present.append(p)
    if not periods_present:
        return None

    # X-axis labels
    if is_pdf:
        x_labels = [str(p).replace("-", "–") for p in periods_present]
    else:
        # Dashboard preference: keep just the year ranges (no "Early century" etc.)
        x_labels = [str(p).replace("-", "–") for p in periods_present]

    sel_scenario_norm = str(sel_scenario).strip().lower()
    sel_period_norm = canonical_period_label(sel_period)

    # Determine y-axis label (metric + units)
    y_label = metric_label
    u = (units or "").strip()
    if u and f"({u})" not in y_label:
        y_label = f"{y_label} ({u})"

    fig = go.Figure()

    # Helper to fetch a value for scenario+period
    def _value_for(scen_norm: str, period_key: str) -> Optional[float]:
        sub = dfp[(dfp["scenario_norm"] == scen_norm) & (dfp["period"] == period_key)]
        if sub.empty:
            return None
        try:
            v = float(pd.to_numeric(sub["value"].iloc[0], errors="coerce"))
        except Exception:
            return None
        return None if pd.isna(v) else v

    # Build grouped bars per scenario.
    #
    # Important: do NOT pass None placeholders for missing scenario/period cells.
    # Plotly reserves bar "slots" even for missing values, which makes the visible
    # bars look shifted left/right relative to the category tick label.
    y_vals_all: list[float] = []
    period_to_label = {p: x_labels[i] for i, p in enumerate(periods_present)}
    outline_width = 1.2

    for scen in SCENARIO_ORDER:
        scen_norm = str(scen).strip().lower()
        if not (dfp["scenario_norm"] == scen_norm).any():
            continue

        x_s: list[str] = []
        y_s: list[float] = []
        line_c: list[str] = []
        text: list[str] = []

        for p in periods_present:
            v = _value_for(scen_norm, p)
            if v is None:
                continue

            x_s.append(period_to_label[p])
            y_s.append(float(v))
            y_vals_all.append(float(v))

            # Keep outline width consistent across all bars; use only a subtle
            # outline color change to indicate the selected cell.
            is_selected = (scen_norm == sel_scenario_norm) and (p == sel_period_norm)
            line_c.append("rgba(0,0,0,0.95)" if is_selected else "rgba(0,0,0,0.65)")

            # Value label formatting (dashboard expectations)
            if u and ("°" in u or "deg" in u.lower() or u.lower() in {"c", "°c"}):
                text.append(f"{float(v):.2f}")
            else:
                text.append(f"{float(v):.2f}")

        if not y_s:
            continue

        fig.add_trace(
            go.Bar(
                x=x_s,
                y=y_s,
                name=SCENARIO_DISPLAY.get(scen_norm, scen_norm),
                marker={
                    "color": SCENARIO_COLORS_HEX.get(scen_norm, "#1f77b4"),
                    "line": {"color": line_c, "width": outline_width},
                },
                text=text,
                textposition="outside",
                cliponaxis=False,
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    + f"Scenario: {SCENARIO_DISPLAY.get(scen_norm, scen_norm)}<br>"
                    + "Value: %{y}" + (f" {u}" if u else "")
                    + "<extra></extra>"
                ),
            )
        )

    zoomed, y_range = compute_scenario_y_range(y_vals_all, y_axis_policy=y_axis_policy)

    apply_irt_plotly_layout(fig, title=None, xaxis_title=None, yaxis_title=y_label)

    fig.update_layout(
        barmode="group",
        bargap=0.35,
        bargroupgap=0.12,
        # Legend at top, horizontal (dashboard polish).
        legend={
            "orientation": "h",
            "x": 0.5,
            "xanchor": "center",
            "y": 1.12,
            "yanchor": "bottom",
        },
        margin={"l": 60, "r": 20, "t": 70, "b": 55},
    )

    if y_range is not None:
        fig.update_yaxes(range=list(y_range))

    # Ensure x-axis categories keep the canonical order and labels are centered.
    fig.update_xaxes(
        type="category",
        categoryorder="array",
        categoryarray=x_labels,
        tickmode="array",
        tickvals=x_labels,
        ticktext=x_labels,
        ticklabelposition="outside",
    )

    return fig


def create_trend_figure_for_index_plotly(
    hist_ts: pd.DataFrame,
    scen_ts: pd.DataFrame,
    idx_label: str,
    scenario_name: str,
    *,
    compare_period_label: Optional[str] = None,
    compare_period_mean: Optional[float] = None,
    units: Optional[str] = None,
    # Optional: per-model series for spaghetti overlay (dashboard)
    model_ts_hist: Optional[pd.DataFrame] = None,
    model_ts_scen: Optional[pd.DataFrame] = None,
    show_model_members: bool = False,
    max_models: int = 15,
    show_band: bool = True,
    # Compatibility kwargs (accepted but ignored; dashboard passes these for Matplotlib parity)
    ax: Any = None,
    figsize: Optional[tuple[float, float]] = None,
    fig_dpi: int = 150,
    font_size_legend: int = 8,
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
    render_context: str = "dashboard",
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

    # Silence unused-arg linters in editors; these are accepted for API compatibility.
    _ = (ax, figsize, fig_dpi, font_size_legend, logo_path, style, render_context)

    def _prep(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()

        # Prefer ensemble median for central tendency to avoid outlier-driven
        # discontinuities between historical and scenario segments.
        if "median" in out.columns and "mean" in out.columns:
            out["mean"] = out["median"]
        elif "ensemble_median" in out.columns and "mean" in out.columns:
            out["mean"] = out["ensemble_median"]

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

    def _prep_models(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        if not {"year", "value", "model"}.issubset(out.columns):
            return pd.DataFrame()
        out["year"] = pd.to_numeric(out["year"], errors="coerce")
        out["value"] = pd.to_numeric(out["value"], errors="coerce")
        out["model"] = out["model"].astype(str)
        out = out.dropna(subset=["year", "value"]).sort_values(["model", "year"]).reset_index(drop=True)
        return out

    hist = _prep(hist_ts)
    scen = _prep(scen_ts)
    models_hist = _prep_models(model_ts_hist) if show_model_members else pd.DataFrame()
    models_scen = _prep_models(model_ts_scen) if show_model_members else pd.DataFrame()

    fig = go.Figure()

    period_lbl = (compare_period_label or "").strip()
    has_compare = (compare_period_mean is not None) and bool(period_lbl)
    u = (units or "").strip()

    # Historical
    if not hist.empty:
        hist_color = SCENARIO_COLORS_HEX["historical"]

        if show_band and ("p05" in hist.columns) and ("p95" in hist.columns):
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
                    fillcolor=_hex_to_rgba(hist_color, 0.18),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

        if not models_hist.empty:
            models = sorted(models_hist["model"].dropna().unique().tolist())
            if max_models and len(models) > int(max_models):
                models = models[: int(max_models)]
            for m in models:
                d = models_hist[models_hist["model"] == m]
                if d.empty:
                    continue
                fig.add_trace(
                    go.Scattergl(
                        x=d["year"],
                        y=d["value"],
                        mode="lines",
                        line={"color": _hex_to_rgba(hist_color, 0.16), "width": 1},
                        meta=str(m),
                        showlegend=False,
                        hovertemplate=(
                            "<b>%{meta}</b><br>"
                            + "Year: %{x:.0f}<br>"
                            + "Value: %{y}" + (f" {u}" if u else "")
                            + "<extra></extra>"
                        ),
                    )
                )

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
                line={"color": hist_color, "width": 2},
                customdata=customdata,
                hovertemplate=hover,
            )
        )

    scen_norm = str(scenario_name).strip().lower()
    scen_color = SCENARIO_COLORS_HEX.get(scen_norm, "#d62728")
    scen_label = SCENARIO_DISPLAY.get(scen_norm, scenario_name)

    if not scen.empty:
        if show_band and ("p05" in scen.columns) and ("p95" in scen.columns):
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
            fig.add_trace(
                go.Scatter(
                    x=scen["year"],
                    y=scen["p05"],
                    mode="lines",
                    line={"width": 0},
                    fill="tonexty",
                    fillcolor=_hex_to_rgba(scen_color, 0.18),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

        if not models_scen.empty:
            models = sorted(models_scen["model"].dropna().unique().tolist())
            if max_models and len(models) > int(max_models):
                models = models[: int(max_models)]
            for m in models:
                d = models_scen[models_scen["model"] == m]
                if d.empty:
                    continue
                fig.add_trace(
                    go.Scattergl(
                        x=d["year"],
                        y=d["value"],
                        mode="lines",
                        line={"color": _hex_to_rgba(scen_color, 0.16), "width": 1},
                        meta=str(m),
                        showlegend=False,
                        hovertemplate=(
                            "<b>%{meta}</b><br>"
                            + "Year: %{x:.0f}<br>"
                            + "Value: %{y}" + (f" {u}" if u else "")
                            + "<extra></extra>"
                        ),
                    )
                )

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

    y_label = idx_label
    if u and f"({u})" not in y_label:
        y_label = f"{y_label} ({u})"

    hovermode = "closest" if show_model_members else "x unified"
    apply_irt_plotly_layout(
        fig,
        title="Trend over time",
        xaxis_title="Year",
        yaxis_title=y_label,
        hovermode=hovermode,
    )

    return fig

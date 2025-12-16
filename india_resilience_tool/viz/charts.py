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
from typing import Any, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# -------------------------
# Scenario / period helpers (index-agnostic)
# -------------------------

SCENARIO_ORDER = ["historical", "ssp245", "ssp585"]
SCENARIO_DISPLAY: dict[str, str] = {
    "historical": "Historical",
    "ssp245": "SSP2-4.5",
    "ssp585": "SSP5-8.5",
}

PERIOD_ORDER = ["1990-2010", "2020-2040", "2040-2060"]


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
    figsize: tuple[float, float] = (4.8, 2.4),
    fig_dpi: int = 150,
    font_size_title: int = 11,
    font_size_label: int = 10,
    font_size_ticks: int = 9,
    font_size_legend: int = 9,
) -> Tuple[Optional[Any], Optional[Any]]:
    """
    Build a compact bar chart showing period-mean values for each scenario.

    Intentionally aligned with the legacy dashboard implementation.
    """
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    if panel_df is None or panel_df.empty:
        return None, None

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

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize, dpi=fig_dpi)
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
        group_labels.append(period)

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

    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.tight_layout()
    return fig, ax


def create_trend_figure_for_index(
    hist_ts: pd.DataFrame,
    scen_ts: pd.DataFrame,
    idx_label: str,
    scenario_name: str,
    ax=None,
    figsize: tuple[float, float] = (4.8, 2.4),
    fig_dpi: int = 150,
    font_size_legend: int = 8,
) -> Any:
    """Create the 'Trend over time' figure (historical + scenario + bands)."""
    import matplotlib.pyplot as plt

    if ax is None:
        fig_ts, ax_ts = plt.subplots(figsize=figsize, dpi=fig_dpi)
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
            linewidth=2.0,
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
                alpha=0.2,
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
            linewidth=2.0,
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
                alpha=0.2,
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
        ax_ts.grid(True, linestyle="--", alpha=0.25)
        for spine in ax_ts.spines.values():
            spine.set_visible(False)
        handles, _labels = ax_ts.get_legend_handles_labels()
        if handles:
            ax_ts.legend(frameon=False, fontsize=font_size_legend, ncol=3)

    if ax is None:
        fig_ts.tight_layout()

    return fig_ts
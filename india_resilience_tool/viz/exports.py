"""
Exports (PDF/ZIP) for IRT.

This module centralizes export builders used by the Streamlit dashboard:
- District case-study PDF (bytes)
- Case-study ZIP bundle (bytes)
- District yearly time-series PDF (saved to disk)

Streamlit-free: UI layer decides caching and button flows.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import difflib
import io
import textwrap
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Mapping, Optional, Union

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

from india_resilience_tool.data.discovery import slugify_fs
from india_resilience_tool.utils.naming import alias
from india_resilience_tool.viz.charts import (
    SCENARIO_DISPLAY,
    create_trend_figure_for_index,
    make_scenario_comparison_figure,
)
from india_resilience_tool.viz.style import (
    IRTFigureStyle,
    add_ra_logo,
    irt_style_context,
    strip_spines,
)

PathLike = Union[str, Path]


def make_case_study_zip(
    *,
    state_name: str,
    district_name: str,
    summary_df: pd.DataFrame,
    ts_dict: dict[str, dict[str, pd.DataFrame]],
    panel_dict: dict[str, pd.DataFrame],
    pdf_bytes: bytes,
    index_label_lookup: Optional[Mapping[str, str]] = None,
) -> bytes:
    """
    Build a ZIP (as bytes) containing:
      - summary.csv
      - timeseries_<index>_<scenario>.csv
      - scenario_mean_<index>.csv
      - climate_profile_<state>__<district>.pdf

    index_label_lookup:
      Optional mapping from index_slug -> human label used in exported CSVs.
      If not provided, label defaults to slug.
    """
    label_map = dict(index_label_lookup or {})

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if summary_df is not None and not summary_df.empty:
            zf.writestr(
                "summary.csv",
                summary_df.to_csv(index=False).encode("utf-8"),
            )

        for slug, parts in (ts_dict or {}).items():
            for scen_key, ts_df in (parts or {}).items():
                if ts_df is None or ts_df.empty:
                    continue
                df_out = ts_df.copy()
                if "scenario" not in df_out.columns:
                    df_out["scenario"] = scen_key
                df_out["index_slug"] = slug
                df_out["index_label"] = label_map.get(slug, slug)
                name = f"timeseries_{slugify_fs(slug)}_{scen_key}.csv"
                zf.writestr(name, df_out.to_csv(index=False).encode("utf-8"))

        for slug, panel_df in (panel_dict or {}).items():
            if panel_df is None or panel_df.empty:
                continue
            df_out = panel_df.copy()
            df_out["index_slug"] = slug
            df_out["index_label"] = label_map.get(slug, slug)
            name = f"scenario_mean_{slugify_fs(slug)}.csv"
            zf.writestr(name, df_out.to_csv(index=False).encode("utf-8"))

        if pdf_bytes:
            safe_state = slugify_fs(state_name)
            safe_dist = slugify_fs(district_name)
            zf.writestr(
                f"climate_profile_{safe_state}__{safe_dist}.pdf",
                pdf_bytes,
            )

    buf.seek(0)
    return buf.getvalue()


def make_district_case_study_pdf(
    *,
    state_name: str,
    district_name: str,
    summary_df: pd.DataFrame,
    ts_dict: dict[str, dict[str, pd.DataFrame]],
    panel_dict: dict[str, pd.DataFrame],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
) -> bytes:
    """
    Build a multi-page PDF for a single district and multiple indices.

    Page 1  : A4 cover + summary table
    Page 2+ : One full A4 page per index with:
        Row 1 – yearly trend plot (historical + scenario)
        Row 2 – period-mean scenario comparison bar chart
        Row 3 – short narrative + scenario bullets

    Notes on styling:
    - A4 pages are portrait by design; we standardize typography and add the RA logo
      consistently at the page level.
    - Charts rendered into sub-axes do not add the logo themselves to avoid duplicates.
    """
    if summary_df is None or summary_df.empty:
        return b""

    s = style or IRTFigureStyle()

    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        # ------------------------------------------------------------------
        # Cover / summary page (A4)
        # ------------------------------------------------------------------
        with irt_style_context(s):
            fig = plt.figure(figsize=(8.27, 11.69), dpi=s.fig_dpi)
            fig.patch.set_facecolor("white")

            title = f"{district_name}, {state_name} — Climate profile"
            fig.text(
                0.5,
                0.92,
                title,
                ha="center",
                va="top",
                fontsize=16,
                fontweight="bold",
            )
            fig.text(
                0.5,
                0.885,
                f"Scenario: {sel_scenario.upper()}   |   Period: {sel_period}   |   Statistic: {sel_stat}",
                ha="center",
                va="top",
                fontsize=10,
            )
            fig.text(
                0.5,
                0.86,
                f"Generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
                ha="center",
                va="top",
                fontsize=8,
            )

            summary_to_show = summary_df.copy()

            # 1) Format numeric columns
            num_cols = ["current", "baseline", "delta_abs", "delta_pct", "percentile_in_state"]
            for c in num_cols:
                if c in summary_to_show.columns:
                    summary_to_show[c] = summary_to_show[c].apply(
                        lambda x: f"{x:.2f}"
                        if isinstance(x, (int, float)) and not pd.isna(x)
                        else ""
                    )

            # 2) Select & order columns
            cols_order = [
                "index_label",
                "group",
                "current",
                "baseline",
                "delta_abs",
                "delta_pct",
                "rank_in_state",
                "percentile_in_state",
                "risk_class",
            ]
            cols_existing = [c for c in cols_order if c in summary_to_show.columns]
            summary_to_show = summary_to_show[cols_existing]

            # 3) Wrap long text so it doesn't overflow table cells
            def _wrap_text(val: object, width: int) -> object:
                if not isinstance(val, str):
                    return val
                return "\n".join(textwrap.wrap(val, width=width)) if val else val

            if "index_label" in summary_to_show.columns:
                summary_to_show["index_label"] = summary_to_show["index_label"].map(
                    lambda x: _wrap_text(x, width=18)
                )
            if "group" in summary_to_show.columns:
                summary_to_show["group"] = summary_to_show["group"].map(
                    lambda x: _wrap_text(x, width=10)
                )
            if "risk_class" in summary_to_show.columns:
                summary_to_show["risk_class"] = summary_to_show["risk_class"].map(
                    lambda x: _wrap_text(x, width=12)
                )

            # Draw table
            ax_table = fig.add_axes([0.05, 0.10, 0.9, 0.70])
            ax_table.axis("off")
            table = ax_table.table(
                cellText=summary_to_show.values,
                colLabels=[c.replace("_", " ").title() for c in summary_to_show.columns],
                loc="center",
                cellLoc="center",
            )
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.scale(0.9, 1.2)

            add_ra_logo(fig, logo_path, width_frac=0.10, pad_frac=0.015, alpha=0.95)

            pdf.savefig(fig)
            plt.close(fig)

        # ------------------------------------------------------------------
        # One A4 per index
        # ------------------------------------------------------------------
        for _, row_idx in summary_df.iterrows():
            slug = str(row_idx.get("index_slug") or "").strip()
            if not slug:
                continue

            idx_label = row_idx.get("index_label", slug)
            units = str(row_idx.get("units") or row_idx.get("unit") or "").strip()

            ts = ts_dict.get(slug, {}) or {}
            hist_ts = ts.get("historical", pd.DataFrame())
            scen_ts = ts.get("scenario", pd.DataFrame())

            panel_df = panel_dict.get(slug)

            with irt_style_context(s):
                fig_idx = plt.figure(figsize=(8.27, 11.69), dpi=s.fig_dpi)
                fig_idx.patch.set_facecolor("white")
                gs = fig_idx.add_gridspec(
                    nrows=3,
                    ncols=1,
                    height_ratios=[3.0, 2.0, 1.3],
                    hspace=0.25,
                )

                fig_idx.suptitle(
                    f"{idx_label} — {district_name}",
                    fontsize=14,
                    fontweight="bold",
                    y=0.98,
                )

                ax_trend = fig_idx.add_subplot(gs[0, 0])
                ax_bar = fig_idx.add_subplot(gs[1, 0])
                ax_text = fig_idx.add_subplot(gs[2, 0])
                ax_text.set_axis_off()

                # 1) Trend figure
                try:
                    if (hist_ts is not None and not hist_ts.empty) or (scen_ts is not None and not scen_ts.empty):
                        create_trend_figure_for_index(
                            hist_ts=hist_ts,
                            scen_ts=scen_ts,
                            idx_label=str(idx_label),
                            scenario_name=sel_scenario,
                            units=units,
                            ax=ax_trend,
                            fig_dpi=s.fig_dpi,
                        )
                    else:
                        ax_trend.text(
                            0.5,
                            0.5,
                            "No yearly time-series data available.",
                            ha="center",
                            va="center",
                            fontsize=9,
                        )
                        ax_trend.set_axis_off()
                except Exception:
                    ax_trend.text(
                        0.5,
                        0.5,
                        "Trend plot could not be generated.",
                        ha="center",
                        va="center",
                        fontsize=9,
                    )
                    ax_trend.set_axis_off()

                # 2) Scenario comparison
                bullet_lines: list[str] = []
                try:
                    if panel_df is not None and not panel_df.empty:
                        make_scenario_comparison_figure(
                            panel_df=panel_df,
                            metric_label=str(idx_label),
                            sel_scenario=sel_scenario,
                            sel_period=sel_period,
                            sel_stat=sel_stat,
                            district_name=district_name,
                            ax=ax_bar,
                        )

                        panel_sorted = panel_df.sort_values(["period", "scenario"])
                        for _, r in panel_sorted.iterrows():
                            scen_label = SCENARIO_DISPLAY.get(
                                str(r["scenario"]).strip().lower(),
                                str(r["scenario"]),
                            )
                            bullet_lines.append(f"• {scen_label} ({r['period']}): {float(r['value']):.2f}")
                    else:
                        ax_bar.text(
                            0.5,
                            0.5,
                            "No scenario panel available.",
                            ha="center",
                            va="center",
                            fontsize=9,
                        )
                        ax_bar.set_axis_off()
                except Exception:
                    ax_bar.text(
                        0.5,
                        0.5,
                        "Scenario comparison could not be generated.",
                        ha="center",
                        va="center",
                        fontsize=9,
                    )
                    ax_bar.set_axis_off()

                # 3) Narrative
                narrative_lines: list[str] = []
                try:
                    combined = pd.DataFrame()
                    if hist_ts is not None and not hist_ts.empty:
                        combined = pd.concat([combined, hist_ts[["year", "mean"]]], ignore_index=True)
                    if scen_ts is not None and not scen_ts.empty:
                        combined = pd.concat([combined, scen_ts[["year", "mean"]]], ignore_index=True)

                    if not combined.empty:
                        combined = combined.copy()
                        combined["year"] = pd.to_numeric(combined["year"], errors="coerce")
                        combined["mean"] = pd.to_numeric(combined["mean"], errors="coerce")
                        combined = combined.dropna(subset=["year", "mean"]).sort_values("year").reset_index(drop=True)

                    if not combined.empty and combined.shape[0] >= 2:
                        start_year = int(combined["year"].iloc[0])
                        end_year = int(combined["year"].iloc[-1])
                        start_val = float(combined["mean"].iloc[0])
                        end_val = float(combined["mean"].iloc[-1])
                        delta = end_val - start_val

                        if abs(delta) < 0.1:
                            trend_word = "has remained broadly stable"
                        elif delta > 0:
                            trend_word = "has increased"
                        else:
                            trend_word = "has decreased"

                        narrative_lines.append(
                            f"Between {start_year} and {end_year}, {str(idx_label).lower()} in {district_name} "
                            f"{trend_word}, from about {start_val:.1f} to about {end_val:.1f}."
                        )
                except Exception:
                    pass

                y_text = 0.95
                if narrative_lines:
                    ax_text.text(
                        0.01,
                        y_text,
                        narrative_lines[0],
                        fontsize=9,
                        va="top",
                        ha="left",
                        transform=ax_text.transAxes,
                    )
                    y_text -= 0.12

                if bullet_lines:
                    ax_text.text(
                        0.01,
                        y_text,
                        "Scenario summary:",
                        fontsize=9,
                        fontweight="bold",
                        va="top",
                        ha="left",
                        transform=ax_text.transAxes,
                    )
                    y_text -= 0.08
                    for line in bullet_lines[:8]:
                        ax_text.text(
                            0.03,
                            y_text,
                            line,
                            fontsize=8,
                            va="top",
                            ha="left",
                            transform=ax_text.transAxes,
                        )
                        y_text -= 0.06

                add_ra_logo(fig_idx, logo_path, width_frac=0.10, pad_frac=0.015, alpha=0.95)

                pdf.savefig(fig_idx)
                plt.close(fig_idx)

    return buf.getvalue()


def make_district_yearly_pdf(
    *,
    df_yearly: pd.DataFrame,
    state_name: str,
    district_name: str,
    scenario_name: str,
    metric_label: str,
    units: Optional[str] = None,
    out_dir: Path,
    logo_path: Optional[PathLike] = None,
    style: Optional[IRTFigureStyle] = None,
) -> Optional[Path]:
    """
    Save a district yearly time-series PDF to disk and return its path.

    Behavior-preserving with the legacy dashboard:
      - requires at least district/scenario/year/mean
      - uses alias() normalization for matching
      - falls back to contains match and fuzzy match within state+scenario

    Styling updates:
      - uses a 16:9 aspect ratio for the figure
      - applies IRT rcParams for consistent typography
      - optionally adds the RA logo
    """
    if df_yearly is None or df_yearly.empty:
        return None

    d = df_yearly.copy()
    cols = set(map(str, d.columns))

    if not {"district", "scenario", "year", "mean"}.issubset(cols):
        return None

    if "state" not in d.columns:
        d["state"] = state_name

    has_p05, has_p95 = ("p05" in d.columns), ("p95" in d.columns)

    def _n(s: str) -> str:
        return alias(s)

    d["_state_key"] = d["state"].astype(str).map(_n)
    d["_district_key"] = d["district"].astype(str).map(_n)
    d["_scen_key"] = d["scenario"].astype(str).str.strip().str.lower()

    scen_key = scenario_name.strip().lower()

    mask = (
        (d["_state_key"] == _n(state_name))
        & (d["_district_key"] == _n(district_name))
        & (d["_scen_key"] == scen_key)
    )

    if not mask.any():
        mask = (
            (d["_state_key"] == _n(state_name))
            & d["_district_key"].str.contains(_n(district_name), na=False)
            & (d["_scen_key"] == scen_key)
        )

    if not mask.any():
        cand = (
            d.loc[(d["_state_key"] == _n(state_name)) & (d["_scen_key"] == scen_key), "_district_key"]
            .dropna()
            .unique()
            .tolist()
        )
        best = difflib.get_close_matches(_n(district_name), cand, n=1, cutoff=0.72)
        if best:
            mask = (
                (d["_state_key"] == _n(state_name))
                & (d["_district_key"] == best[0])
                & (d["_scen_key"] == scen_key)
            )

    if not mask.any():
        return None

    d = d.loc[mask].copy()
    d["year"] = pd.to_numeric(d["year"], errors="coerce")
    d["mean"] = pd.to_numeric(d["mean"], errors="coerce")
    if has_p05:
        d["p05"] = pd.to_numeric(d["p05"], errors="coerce")
    if has_p95:
        d["p95"] = pd.to_numeric(d["p95"], errors="coerce")

    d = d.dropna(subset=["year", "mean"]).sort_values("year").reset_index(drop=True)
    if d.empty:
        return None

    s = style or IRTFigureStyle()

    # Keep a similar visual footprint to legacy (width ~10.5 inches), but enforce 16:9.
    fig_w = 10.5
    fig_h = fig_w * (9.0 / 16.0)

    with irt_style_context(s):
        fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=s.fig_dpi)
        ax.plot(d["year"].to_numpy(), d["mean"].to_numpy(), linewidth=2.2, label="Mean")

        if has_p05 and has_p95 and d["p05"].notna().any() and d["p95"].notna().any():
            ax.fill_between(
                d["year"].to_numpy(),
                d["p05"].to_numpy(),
                d["p95"].to_numpy(),
                alpha=0.25,
                label="P05–P95",
            )

        ax.set_title(f"{metric_label} — {district_name}, {state_name} ({scenario_name.upper()})", fontsize=12)
        ax.set_xlabel("Year")
        u = (units or "").strip()
        y_label = metric_label
        if u and f"({u})" not in y_label:
            y_label = f"{y_label} ({u})"
        ax.set_ylabel(y_label)
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(frameon=False, ncol=3, fontsize=9)

        strip_spines(ax)

        out_dir.mkdir(parents=True, exist_ok=True)

        def _safe(s_in: str) -> str:
            return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(s_in))

        pdf_path = (
            out_dir
            / f"{_safe(state_name)}__{_safe(district_name)}__"
              f"{_safe(metric_label)}__{_safe(scenario_name)}__yearly_timeseries.pdf"
        )

        try:
            fig.tight_layout()
        except Exception:
            pass

        add_ra_logo(fig, logo_path, width_frac=0.12, pad_frac=0.012, alpha=0.95)

        fig.savefig(pdf_path, format="pdf")
        plt.close(fig)

    return pdf_path

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

from collections import Counter

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
            # Page 1 is a compact executive summary that scales to large metric selections.
            # The full metric table is moved to an appendix at the end of the PDF.
            fig = plt.figure(figsize=(8.27, 11.69), dpi=s.fig_dpi)
            fig.patch.set_facecolor("white")

            scenario_disp = SCENARIO_DISPLAY.get(str(sel_scenario).strip().lower(), str(sel_scenario))
            stat_disp = str(sel_stat).strip().title()

            # Derive a concise baseline period descriptor (if the case-study builder provides it).
            baseline_line = ""
            if "baseline_period" in summary_df.columns:
                periods_raw = [
                    str(p).strip().replace("_", "-")
                    for p in summary_df["baseline_period"].dropna().tolist()
                    if str(p).strip()
                ]
                if periods_raw:
                    counts = Counter(periods_raw)
                    most_common_period, _ = counts.most_common(1)[0]
                    if len(counts) == 1:
                        baseline_line = f"Baseline (historical): {most_common_period}"
                    else:
                        baseline_line = f"Baseline (historical): {most_common_period} (mixed periods)"

            # ------------------------------------------------------------------
            # Header
            # ------------------------------------------------------------------
            ax_header = fig.add_axes([0.06, 0.86, 0.88, 0.12])
            ax_header.axis("off")

            title = f"{district_name}, {state_name} — Climate profile"
            ax_header.text(
                0.5,
                0.92,
                title,
                ha="center",
                va="top",
                fontsize=16,
                fontweight="bold",
            )
            ax_header.text(
                0.5,
                0.60,
                f"Scenario: {scenario_disp}   |   Period: {sel_period}   |   Statistic: {stat_disp}",
                ha="center",
                va="top",
                fontsize=10,
            )
            if baseline_line:
                ax_header.text(
                    0.5,
                    0.38,
                    baseline_line,
                    ha="center",
                    va="top",
                    fontsize=9,
                )
            ax_header.text(
                0.5,
                0.14,
                f"Generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
                ha="center",
                va="top",
                fontsize=8,
            )

            # ------------------------------------------------------------------
            # Executive summary blocks (2 x 2)
            # ------------------------------------------------------------------
            df0 = summary_df.copy()

            df0["group_disp"] = (
                df0.get("group", pd.Series(["Unknown"] * len(df0)))
                .fillna("Unknown")
                .astype(str)
                .str.replace("_", " ", regex=False)
                .str.strip()
                .replace("", "Unknown")
                .str.title()
            )
            df0["risk_disp"] = (
                df0.get("risk_class", pd.Series(["Unknown"] * len(df0)))
                .fillna("Unknown")
                .astype(str)
                .str.strip()
                .replace("", "Unknown")
            )

            total_metrics = int(len(df0))
            baseline_available = int(pd.to_numeric(df0.get("baseline", pd.Series([])), errors="coerce").notna().sum())
            baseline_missing = max(total_metrics - baseline_available, 0)

            group_counts = df0["group_disp"].value_counts().sort_index()
            group_counts_df = (
                group_counts.rename_axis("Group")
                .reset_index(name="Metrics")
                .sort_values("Metrics", ascending=False)
                .reset_index(drop=True)
            )

            risk_order = ["Very High", "High", "Medium", "Low", "Very Low", "Unknown"]
            risk_counts = df0["risk_disp"].value_counts()
            risk_counts_df = pd.DataFrame(
                {
                    "Risk": risk_order,
                    "Count": [int(risk_counts.get(k, 0)) for k in risk_order],
                }
            )
            risk_counts_df = risk_counts_df[risk_counts_df["Count"] > 0].reset_index(drop=True)

            def _sev(r: str) -> int:
                m = {"Very High": 5, "High": 4, "Medium": 3, "Low": 2, "Very Low": 1, "Unknown": 0}
                return int(m.get(str(r).strip(), 0))

            df0["_sev"] = df0["risk_disp"].map(_sev)
            df0["_pct"] = pd.to_numeric(df0.get("percentile_in_state", pd.Series([None] * len(df0))), errors="coerce")

            top_risk = (
                df0.sort_values(["_sev", "_pct"], ascending=[False, False])
                .head(5)
                .copy()
            )
            top_risk_df = pd.DataFrame(
                {
                    "Metric": top_risk.get("index_label", top_risk.get("index_slug", "")).astype(str),
                    "Risk": top_risk["risk_disp"].astype(str),
                }
            )

            # Largest changes vs baseline (Δ Abs). Use absolute magnitude but keep signed value in display.
            df0["_dabs"] = pd.to_numeric(df0.get("delta_abs", pd.Series([None] * len(df0))), errors="coerce")
            df0["_abs_dabs"] = df0["_dabs"].abs()
            top_delta = (
                df0.dropna(subset=["_dabs"])
                .sort_values("_abs_dabs", ascending=False)
                .head(5)
                .copy()
            )

            def _delta_str(v: float) -> str:
                try:
                    return f"{float(v):.2f}"
                except Exception:
                    return ""

            top_delta_df = pd.DataFrame(
                {
                    "Metric": top_delta.get("index_label", top_delta.get("index_slug", "")).astype(str),
                    "Δ Abs": top_delta["_dabs"].map(_delta_str),
                }
            )

            def _wrap(s: str, width: int) -> str:
                if not s:
                    return ""
                wrapper = textwrap.TextWrapper(width=width, break_long_words=False, break_on_hyphens=False)
                return "\n".join(wrapper.wrap(str(s)))

            def _draw_table(
                ax,
                df: pd.DataFrame,
                *,
                title: str,
                font_size: int = 8,
                wrap_cols: Optional[dict[str, int]] = None,
                col_widths: Optional[list[float]] = None,
            ) -> None:
                ax.axis("off")
                ax.text(0.0, 1.02, title, ha="left", va="bottom", fontsize=10, fontweight="bold")
                df_show = df.copy()
                wrap_cols = wrap_cols or {}
                for col, w in wrap_cols.items():
                    if col in df_show.columns:
                        df_show[col] = df_show[col].map(lambda x: _wrap(str(x), w))
                table = ax.table(
                    cellText=df_show.values,
                    colLabels=list(df_show.columns),
                    colWidths=col_widths,
                    cellLoc="left",
                    colLoc="left",
                    bbox=[0.0, 0.0, 1.0, 0.95],
                )
                table.auto_set_font_size(False)
                table.set_fontsize(font_size)
                for (ri, ci), cell in table.get_celld().items():
                    cell.set_edgecolor((0.85, 0.85, 0.85, 1.0))
                    cell.set_linewidth(0.6)
                    cell.PAD = 0.02
                    if ri == 0:
                        cell.set_facecolor((0.95, 0.95, 0.95, 1.0))
                        cell.get_text().set_fontweight("bold")
                        cell.get_text().set_ha("left")
                    else:
                        cell.set_facecolor((1.0, 1.0, 1.0, 1.0) if (ri % 2 == 0) else (0.98, 0.98, 0.98, 1.0))
                        cell.get_text().set_ha("left")

            gs = fig.add_gridspec(
                nrows=2,
                ncols=2,
                left=0.06,
                right=0.94,
                bottom=0.18,
                top=0.83,
                wspace=0.20,
                hspace=0.28,
            )
            ax_cov = fig.add_subplot(gs[0, 0])
            ax_risk = fig.add_subplot(gs[0, 1])
            ax_top = fig.add_subplot(gs[1, 0])
            ax_delta = fig.add_subplot(gs[1, 1])

            cov_title = f"Coverage (total metrics: {total_metrics})"
            _draw_table(ax_cov, group_counts_df, title=cov_title, font_size=8)

            risk_title = "Risk distribution"
            _draw_table(ax_risk, risk_counts_df, title=risk_title, font_size=8)

            ax_risk.text(
                0.0,
                -0.10,
                f"Baseline available for {baseline_available}/{total_metrics} metrics"
                + (f" (missing for {baseline_missing})" if baseline_missing else ""),
                transform=ax_risk.transAxes,
                ha="left",
                va="top",
                fontsize=8,
                color="black",
            )

            _draw_table(
                ax_top,
                top_risk_df,
                title="Top risk drivers (highest severity)",
                font_size=8,
                wrap_cols={"Metric": 44},
                col_widths=[0.78, 0.22],
            )
            if top_delta_df.empty:
                ax_delta.axis("off")
                ax_delta.text(
                    0.0,
                    1.02,
                    "Largest changes vs baseline",
                    ha="left",
                    va="bottom",
                    fontsize=10,
                    fontweight="bold",
                )
                ax_delta.text(
                    0.0,
                    0.85,
                    "No baseline deltas available.",
                    ha="left",
                    va="top",
                    fontsize=9,
                )
            else:
                _draw_table(
                    ax_delta,
                    top_delta_df,
                    title="Largest changes vs baseline (Δ Abs)",
                    font_size=8,
                    wrap_cols={"Metric": 44},
                    col_widths=[0.82, 0.18],
                )

            ax_footer = fig.add_axes([0.06, 0.08, 0.88, 0.08])
            ax_footer.axis("off")
            ax_footer.text(
                0.0,
                0.80,
                "Full metric-by-metric table is provided in Appendix A (end of report).",
                ha="left",
                va="top",
                fontsize=9,
            )
            ax_footer.text(
                0.0,
                0.40,
                "Tip: Use the per-metric pages to see trend + scenario comparisons for each index.",
                ha="left",
                va="top",
                fontsize=8,
            )

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


        # ------------------------------------------------------------------
        # Appendix A: Full metric summary table (may span multiple pages)
        # ------------------------------------------------------------------
        with irt_style_context(s):
            df_full = summary_df.copy()

            def _wrap_text(val: str, width: int) -> str:
                if not val:
                    return ""
                wrapper = textwrap.TextWrapper(width=width, break_long_words=False, break_on_hyphens=False)
                return "\n".join(wrapper.wrap(str(val)))

            def _fmt_float(x: object, decimals: int) -> str:
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return ""
                try:
                    return f"{float(x):.{decimals}f}"
                except Exception:
                    return ""

            def _fmt_int(x: object) -> str:
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return ""
                try:
                    return str(int(round(float(x))))
                except Exception:
                    return ""

            def _decimals_for_units(u: str) -> int:
                u = (u or "").strip()
                ul = u.lower()
                if not u:
                    return 2
                if "°" in u or "deg" in ul or ul in {"c", "°c"}:
                    return 1
                if "day" in ul or ul in {"days"}:
                    return 0
                if "%" in u:
                    return 1
                return 2

            # Choose stable columns; keep appendix concise but complete.
            col_specs = [
                {"key": "index_label", "label": "Index", "ha": "left", "wrap": 46, "w": 0.36},
                {"key": "group", "label": "Group", "ha": "left", "wrap": 14, "w": 0.10},
                {"key": "current", "label": "Current", "ha": "right", "wrap": 0, "w": 0.08},
                {"key": "baseline", "label": "Baseline", "ha": "right", "wrap": 0, "w": 0.08},
                {"key": "delta_abs", "label": "Δ Abs", "ha": "right", "wrap": 0, "w": 0.07},
                {"key": "delta_pct", "label": "Δ %", "ha": "right", "wrap": 0, "w": 0.06},
                {"key": "rank_in_state", "label": "Rank", "ha": "right", "wrap": 0, "w": 0.06},
                {"key": "percentile_in_state", "label": "Pctile", "ha": "right", "wrap": 0, "w": 0.07},
                {"key": "risk_class", "label": "Risk", "ha": "center", "wrap": 10, "w": 0.12},
            ]
            available_cols = set(df_full.columns)
            col_specs = [c for c in col_specs if c["key"] in available_cols]

            # Paginate rows to prevent overflow. We also start a new page per group/bundle
            # so that (e.g.) Temperature, Precipitation, Drought appear as separate sections.
            rows_per_page = 28

            group_key = "bundle" if "bundle" in df_full.columns else "group"
            if group_key in df_full.columns:
                df_full["_group_disp"] = (
                    df_full[group_key]
                    .fillna("All Metrics")
                    .astype(str)
                    .str.replace("_", " ", regex=False)
                    .str.strip()
                    .replace("", "All Metrics")
                    .str.title()
                )
            else:
                df_full["_group_disp"] = "All Metrics"

            group_order = [g for g in pd.unique(df_full["_group_disp"]) if str(g).strip()]
            if not group_order:
                group_order = ["All Metrics"]

            group_pages: list[tuple[str, pd.DataFrame, int]] = []
            for g in group_order:
                gdf = df_full[df_full["_group_disp"] == g].reset_index(drop=True)
                pages = max((len(gdf) + rows_per_page - 1) // rows_per_page, 1)
                group_pages.append((str(g), gdf, pages))

            n_pages_total = sum(pages for _, _, pages in group_pages)
            page_global = 0

            header_face = (0.95, 0.95, 0.95, 1.0)
            zebra_a = (1.0, 1.0, 1.0, 1.0)
            zebra_b = (0.98, 0.98, 0.98, 1.0)
            edge = (0.80, 0.80, 0.80, 1.0)

            for group_name, gdf, n_pages in group_pages:
                for page_i in range(n_pages):
                    page_global += 1
                    start = page_i * rows_per_page
                    end = min(start + rows_per_page, len(gdf))
                    chunk = gdf.iloc[start:end].copy()

                # Build cell text (row-specific unit-aware formatting).
                cell_text: list[list[str]] = []
                for _, r in chunk.iterrows():
                    units = str(r.get("units") or r.get("unit") or "").strip()
                    dec = _decimals_for_units(units)
                    idx_label = str(r.get("index_label") or r.get("index_slug") or "").strip()
                    if units and units not in idx_label:
                        idx_label = f"{idx_label} ({units})"

                    row_cells: list[str] = []
                    for c in col_specs:
                        key = c["key"]
                        if key == "index_label":
                            row_cells.append(_wrap_text(idx_label, c["wrap"]))
                        elif key == "group":
                            grp = str(r.get("group") or "").strip()
                            grp_disp = grp.replace("_", " ").title() if grp else ""
                            row_cells.append(_wrap_text(grp_disp, c["wrap"]) if c["wrap"] else grp_disp)
                        elif key in {"current", "baseline", "delta_abs"}:
                            row_cells.append(_fmt_float(r.get(key), dec))
                        elif key == "delta_pct":
                            row_cells.append(_fmt_float(r.get(key), 1))
                        elif key == "percentile_in_state":
                            row_cells.append(_fmt_float(r.get(key), 1))
                        elif key == "rank_in_state":
                            row_cells.append(_fmt_int(r.get(key)))
                        elif key == "risk_class":
                            rc = str(r.get("risk_class") or "").strip()
                            row_cells.append(_wrap_text(rc, c["wrap"]) if c["wrap"] else rc)
                        else:
                            row_cells.append(str(r.get(key) or "").strip())
                    cell_text.append(row_cells)

                col_labels = [c["label"] for c in col_specs]
                col_widths = [c["w"] for c in col_specs]

                # A4 landscape for appendix tables.
                fig_app = plt.figure(figsize=(11.69, 8.27), dpi=s.fig_dpi)
                fig_app.patch.set_facecolor("white")

                ax_head = fig_app.add_axes([0.06, 0.88, 0.88, 0.10])
                ax_head.axis("off")
                ax_head.text(
                    0.0,
                    0.85,
                    f"Appendix A — Full metric summary table ({page_global}/{n_pages_total})",
                    ha="left",
                    va="top",
                    fontsize=12,
                    fontweight="bold",
                )
                ax_head.text(
                    0.0,
                    0.58,
                    f"Group: {group_name} ({page_i + 1}/{n_pages})",
                    ha="left",
                    va="top",
                    fontsize=9,
                )

                ax_head.text(
                    0.0,
                    0.35,
                    f"{district_name}, {state_name}   |   Scenario: {scenario_disp}   |   Period: {sel_period}   |   Stat: {stat_disp}",
                    ha="left",
                    va="top",
                    fontsize=9,
                )

                ax_table = fig_app.add_axes([0.06, 0.08, 0.88, 0.78])
                ax_table.axis("off")

                body_font = 8
                if len(chunk) >= 26:
                    body_font = 7

                table = ax_table.table(
                    cellText=cell_text,
                    colLabels=col_labels,
                    colWidths=col_widths,
                    cellLoc="center",
                    bbox=[0.0, 0.0, 1.0, 1.0],
                )
                table.auto_set_font_size(False)
                table.set_fontsize(body_font)
                table.scale(1.0, 1.25)

                for (row_i, col_i), cell in table.get_celld().items():
                    cell.set_edgecolor(edge)
                    cell.set_linewidth(0.6)
                    cell.PAD = 0.02
                    if row_i == 0:
                        cell.set_facecolor(header_face)
                        cell.set_linewidth(0.9)
                        cell.get_text().set_fontweight("bold")
                        cell.get_text().set_ha("center")
                    else:
                        cell.set_facecolor(zebra_b if (row_i % 2) else zebra_a)
                        ha = col_specs[col_i]["ha"]
                        cell.get_text().set_ha(ha)

                add_ra_logo(fig_app, logo_path, width_frac=0.10, pad_frac=0.015, alpha=0.95)

                pdf.savefig(fig_app)
                plt.close(fig_app)


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

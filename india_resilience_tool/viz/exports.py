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
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

from india_resilience_tool.data.discovery import slugify_fs
from india_resilience_tool.utils.naming import alias
from india_resilience_tool.viz.charts import (
    SCENARIO_DISPLAY,
    create_trend_figure_for_index,
    make_scenario_comparison_figure,
)
from india_resilience_tool.viz.formatting import format_metric_compact, get_metric_display_units
from india_resilience_tool.viz.style import (
    IRTFigureStyle,
    add_ra_logo,
    irt_style_context,
    strip_spines,
)

PathLike = Union[str, Path]


def _wrap_to_n_lines(text: str, *, width: int, max_lines: int = 2) -> str:
    """Wrap text to at most N lines; truncate with ellipsis if needed."""
    t = str(text or "").strip()
    if not t:
        return ""
    wrapper = textwrap.TextWrapper(width=width, break_long_words=False, break_on_hyphens=False)
    lines = wrapper.wrap(t)
    if len(lines) <= max_lines:
        return "\n".join(lines)
    kept = lines[:max_lines]
    # Truncate the last line with an ellipsis while keeping it readable.
    last = kept[-1].rstrip()
    if len(last) > 4:
        last = last[:-3].rstrip()
    kept[-1] = f"{last}..."
    return "\n".join(kept)


def _draw_pdf_header(
    *,
    fig: plt.Figure,
    title: str,
    subtitle_lines: list[str],
    logo_path: Optional[PathLike],
    is_cover: bool,
    show_logo: bool,
) -> None:
    """Draw a reserved header band to avoid collisions with the logo."""
    if is_cover:
        ax = fig.add_axes([0.06, 0.86, 0.76, 0.12])
        title_fs = 16
        sub_fs = 10
        title_width = 62
    else:
        ax = fig.add_axes([0.06, 0.915, 0.88, 0.075])
        title_fs = 14
        sub_fs = 9
        title_width = 78

    ax.axis("off")
    ax.text(
        0.0,
        0.95,
        _wrap_to_n_lines(title, width=title_width, max_lines=2),
        ha="left",
        va="top",
        fontsize=title_fs,
        fontweight="bold",
    )

    y = 0.55 if is_cover else 0.35
    for line in [ln for ln in subtitle_lines if str(ln or "").strip()]:
        ax.text(
            0.0,
            y,
            str(line),
            ha="left",
            va="top",
            fontsize=sub_fs,
        )
        y -= 0.22 if is_cover else 0.20

    if show_logo:
        # Keep the logo out of the title region by reserving header space.
        add_ra_logo(fig, logo_path, width_frac=0.10 if is_cover else 0.08, pad_frac=0.012, alpha=0.95, position="top_right")


def _draw_pdf_footer(
    *,
    fig: plt.Figure,
    page_no: int,
    total_pages: int,
    logo_path: Optional[PathLike],
    show_logo: bool,
    left_text: str = "India Resilience Tool — Climate profile",
) -> None:
    """Draw a consistent footer with page numbers (and optional small logo)."""
    y = 0.018
    fig.text(0.06, y, left_text, ha="left", va="bottom", fontsize=8)

    # Leave room for the footer logo if we render it.
    right_x = 0.88 if show_logo else 0.94
    fig.text(right_x, y, f"Page {page_no} of {total_pages}", ha="right", va="bottom", fontsize=8)

    if show_logo:
        add_ra_logo(fig, logo_path, width_frac=0.08, pad_frac=0.012, alpha=0.95, position="footer_right")


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

    Structure:
      Page 1: Cover / Executive summary
      Page 2: Contents (Table of contents)
      Then:   Bundle divider + per-metric pages (one page per metric)
      Last:   Appendix A (full metric summary table; paginated)

    Notes on styling:
    - A4 pages are portrait by design; we standardize typography and add the RA logo
      consistently at the page level.
    - Charts rendered into sub-axes do not add the logo themselves to avoid duplicates.
    """
    if summary_df is None or summary_df.empty:
        return b""

    # Normalize schema differences between legacy dashboard summaries and the PDF exporter.
    # This keeps the exporter backward-compatible while avoiding brittle indexing errors.
    summary_df = summary_df.copy()

    if "index_slug" not in summary_df.columns and "slug" in summary_df.columns:
        summary_df["index_slug"] = summary_df["slug"]

    if "index_label" not in summary_df.columns:
        if "index" in summary_df.columns:
            summary_df["index_label"] = summary_df["index"]
        elif "label" in summary_df.columns:
            summary_df["index_label"] = summary_df["label"]

    if "units" not in summary_df.columns and "unit" in summary_df.columns:
        summary_df["units"] = summary_df["unit"]

    if "delta_abs" not in summary_df.columns and "delta_vs_baseline" in summary_df.columns:
        summary_df["delta_abs"] = summary_df["delta_vs_baseline"]

    if "delta_pct" not in summary_df.columns and "baseline" in summary_df.columns and "delta_abs" in summary_df.columns:
        baseline_num = pd.to_numeric(summary_df["baseline"], errors="coerce")
        delta_num = pd.to_numeric(summary_df["delta_abs"], errors="coerce")
        with np.errstate(divide="ignore", invalid="ignore"):
            pct = (delta_num / baseline_num) * 100.0
        pct = pct.where(baseline_num.notna() & (baseline_num != 0), np.nan)
        summary_df["delta_pct"] = pct

    if "percentile_in_state" not in summary_df.columns and "percentile_state" in summary_df.columns:
        summary_df["percentile_in_state"] = summary_df["percentile_state"]

    s = style or IRTFigureStyle()

    # ------------------------------------------------------------------
    # Helpers (local to keep patch scope tight)
    # ------------------------------------------------------------------

    def _wrap_text(val: object, width: int) -> str:
        t = str(val or "").strip()
        if not t:
            return ""
        wrapper = textwrap.TextWrapper(width=width, break_long_words=False, break_on_hyphens=False)
        return "\n".join(wrapper.wrap(t))

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
                df_show[col] = df_show[col].map(lambda x: _wrap_text(x, w))

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

    # ------------------------------------------------------------------
    # Page planning (for consistent footers with page numbers + TOC)
    # ------------------------------------------------------------------
    # `summary_df` may come from different producers (legacy dashboard vs case-study builder).
    # Always build an index-aligned mask to avoid pandas "Unalignable boolean Series" errors.
    if "index_slug" in summary_df.columns:
        slugs_series = summary_df["index_slug"]
    elif "slug" in summary_df.columns:
        slugs_series = summary_df["slug"]
    else:
        slugs_series = pd.Series([""] * len(summary_df), index=summary_df.index, dtype=str)

    slugs_series = slugs_series.fillna("").astype(str).str.strip()
    mask_metrics = slugs_series.ne("")
    df_metrics = summary_df.loc[mask_metrics].copy()
    n_index_pages = int(len(df_metrics))

    bundle_key = "bundle" if "bundle" in df_metrics.columns else "group"
    if bundle_key in df_metrics.columns:
        df_metrics["_bundle_disp"] = (
            df_metrics[bundle_key]
            .fillna("All Metrics")
            .astype(str)
            .str.replace("_", " ", regex=False)
            .str.strip()
            .replace("", "All Metrics")
            .str.title()
        )
    else:
        df_metrics["_bundle_disp"] = "All Metrics"

    bundle_order = [b for b in pd.unique(df_metrics["_bundle_disp"]) if str(b).strip()]
    if not bundle_order:
        bundle_order = ["All Metrics"]

    bundle_counts = {b: int((df_metrics["_bundle_disp"] == b).sum()) for b in bundle_order}
    n_bundle_dividers = int(len(bundle_order))

    # Appendix A page count depends on metric count (and grouping by bundle/group).
    rows_per_page = 28
    df_full_for_plan = summary_df.copy()
    group_key = "bundle" if "bundle" in df_full_for_plan.columns else "group"
    if group_key in df_full_for_plan.columns:
        df_full_for_plan["_group_disp"] = (
            df_full_for_plan[group_key]
            .fillna("All Metrics")
            .astype(str)
            .str.replace("_", " ", regex=False)
            .str.strip()
            .replace("", "All Metrics")
            .str.title()
        )
    else:
        df_full_for_plan["_group_disp"] = "All Metrics"

    group_order = [g for g in pd.unique(df_full_for_plan["_group_disp"]) if str(g).strip()] or ["All Metrics"]
    group_pages_plan: list[tuple[str, pd.DataFrame, int]] = []
    for g in group_order:
        gdf = df_full_for_plan[df_full_for_plan["_group_disp"] == g].reset_index(drop=True)
        pages = max((len(gdf) + rows_per_page - 1) // rows_per_page, 1)
        group_pages_plan.append((str(g), gdf, pages))

    n_appendix_pages_total = int(sum(p for _, _, p in group_pages_plan))
    # Cover + TOC + bundle dividers + metric pages + appendix pages
    total_pages = 2 + n_bundle_dividers + n_index_pages + n_appendix_pages_total

    # Compute TOC page mapping (bundle divider pages are the section starts).
    bundle_start_pages: dict[str, int] = {}
    bundle_end_pages: dict[str, int] = {}
    page_cursor = 3  # after cover (1) and contents (2)
    for b in bundle_order:
        start_p = page_cursor
        end_p = start_p + 1 + max(bundle_counts.get(b, 0), 0) - 1
        bundle_start_pages[b] = start_p
        bundle_end_pages[b] = max(end_p, start_p)
        page_cursor = bundle_end_pages[b] + 1

    appendix_start_page = page_cursor
    appendix_end_page = appendix_start_page + max(n_appendix_pages_total, 1) - 1

    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        # Shared display strings
        scenario_disp = SCENARIO_DISPLAY.get(str(sel_scenario).strip().lower(), str(sel_scenario))
        stat_disp = str(sel_stat).strip().title()

        page_no = 1

        # ------------------------------------------------------------------
        # Page 1: Cover / executive summary (A4 portrait)
        # ------------------------------------------------------------------
        with irt_style_context(s):
            fig = plt.figure(figsize=(8.27, 11.69), dpi=s.fig_dpi)
            fig.patch.set_facecolor("white")
            fig.subplots_adjust(bottom=0.06)

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

            title = f"{district_name}, {state_name} — Climate profile"
            subtitle_lines: list[str] = [
                f"Scenario: {scenario_disp}   |   Period: {sel_period}   |   Statistic: {stat_disp}",
            ]
            if baseline_line:
                subtitle_lines.append(baseline_line)
            subtitle_lines.append(f"Generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

            _draw_pdf_header(
                fig=fig,
                title=title,
                subtitle_lines=subtitle_lines,
                logo_path=logo_path,
                is_cover=True,
                show_logo=True,
            )

            df0 = summary_df.copy()

            cov_key = "bundle" if "bundle" in df0.columns else "group"
            if cov_key in df0.columns:
                df0["bundle_disp"] = (
                    df0[cov_key]
                    .fillna("Unknown")
                    .astype(str)
                    .str.replace("_", " ", regex=False)
                    .str.strip()
                    .replace("", "Unknown")
                    .str.title()
                )
            else:
                df0["bundle_disp"] = "Unknown"

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

            bundle_counts_df = (
                df0["bundle_disp"]
                .value_counts()
                .rename_axis("Bundle")
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

            top_risk = df0.sort_values(["_sev", "_pct"], ascending=[False, False]).head(5).copy()
            top_risk_df = pd.DataFrame(
                {
                    "Metric": top_risk.get("index_label", top_risk.get("index_slug", "")).astype(str),
                    "Risk": top_risk["risk_disp"].astype(str),
                }
            )

            df0["_dabs"] = pd.to_numeric(df0.get("delta_abs", pd.Series([None] * len(df0))), errors="coerce")
            df0["_abs_dabs"] = df0["_dabs"].abs()
            top_delta = df0.dropna(subset=["_dabs"]).sort_values("_abs_dabs", ascending=False).head(5).copy()

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
            _draw_table(ax_cov, bundle_counts_df, title=cov_title, font_size=8)

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
                "Use the per-metric pages for trend + scenario comparisons for each index.",
                ha="left",
                va="top",
                fontsize=8,
            )

            _draw_pdf_footer(
                fig=fig,
                page_no=page_no,
                total_pages=total_pages,
                logo_path=logo_path,
                show_logo=False,
            )

            pdf.savefig(fig)
            plt.close(fig)
            page_no += 1

        # ------------------------------------------------------------------
        # Page 2: Contents (Table of contents)
        # ------------------------------------------------------------------
        with irt_style_context(s):
            fig_toc = plt.figure(figsize=(8.27, 11.69), dpi=s.fig_dpi)
            fig_toc.patch.set_facecolor("white")
            fig_toc.subplots_adjust(bottom=0.06)

            _draw_pdf_header(
                fig=fig_toc,
                title="Contents",
                subtitle_lines=[
                    f"{district_name}, {state_name}",
                ],
                logo_path=logo_path,
                is_cover=False,
                show_logo=False,
            )

            rows: list[dict[str, str]] = []
            for b in bundle_order:
                sp = bundle_start_pages.get(b, 0)
                ep = bundle_end_pages.get(b, sp)
                pages_s = f"{sp}–{ep}" if sp and ep and ep != sp else (str(sp) if sp else "")
                rows.append({"Section": str(b), "Start": str(sp), "Pages": pages_s})

            rows.append(
                {
                    "Section": "Appendix A — Full metric summary table",
                    "Start": str(appendix_start_page),
                    "Pages": f"{appendix_start_page}–{appendix_end_page}"
                    if appendix_end_page != appendix_start_page
                    else str(appendix_start_page),
                }
            )
            toc_df = pd.DataFrame(rows)

            ax_tbl = fig_toc.add_axes([0.06, 0.14, 0.88, 0.72])
            _draw_table(
                ax_tbl,
                toc_df,
                title="",
                font_size=9,
                wrap_cols={"Section": 56},
                col_widths=[0.68, 0.12, 0.20],
            )

            _draw_pdf_footer(
                fig=fig_toc,
                page_no=page_no,
                total_pages=total_pages,
                logo_path=logo_path,
                show_logo=True,
            )
            pdf.savefig(fig_toc)
            plt.close(fig_toc)
            page_no += 1

        # ------------------------------------------------------------------
        # Bundle sections: divider + one A4 per metric (portrait)
        # ------------------------------------------------------------------
        def _bundle_risk_counts(df_b: pd.DataFrame) -> pd.DataFrame:
            risk_order_local = ["Very High", "High", "Medium", "Low", "Very Low", "Unknown"]
            rc = (
                df_b.get("risk_class", pd.Series(["Unknown"] * len(df_b)))
                .fillna("Unknown")
                .astype(str)
                .str.strip()
                .replace("", "Unknown")
            )
            counts = rc.value_counts()
            out = pd.DataFrame(
                {"Risk": risk_order_local, "Count": [int(counts.get(k, 0)) for k in risk_order_local]}
            )
            out = out[out["Count"] > 0].reset_index(drop=True)
            return out

        def _bundle_top_risk(df_b: pd.DataFrame) -> pd.DataFrame:
            dfx = df_b.copy()
            rc = (
                dfx.get("risk_class", pd.Series(["Unknown"] * len(dfx)))
                .fillna("Unknown")
                .astype(str)
                .str.strip()
                .replace("", "Unknown")
            )
            dfx["_sev"] = rc.map(lambda r: {"Very High": 5, "High": 4, "Medium": 3, "Low": 2, "Very Low": 1}.get(str(r).strip(), 0))
            dfx["_pct"] = pd.to_numeric(dfx.get("percentile_in_state", pd.Series([None] * len(dfx))), errors="coerce")
            df_top = dfx.sort_values(["_sev", "_pct"], ascending=[False, False]).head(4).copy()
            return pd.DataFrame(
                {
                    "Metric": df_top.get("index_label", df_top.get("index_slug", "")).astype(str),
                    "Risk": rc.loc[df_top.index].astype(str),
                }
            )

        def _bundle_top_deltas(df_b: pd.DataFrame) -> pd.DataFrame:
            dfx = df_b.copy()
            dfx["_dabs"] = pd.to_numeric(dfx.get("delta_abs", pd.Series([None] * len(dfx))), errors="coerce")
            dfx["_abs_dabs"] = dfx["_dabs"].abs()
            df_top = dfx.dropna(subset=["_dabs"]).sort_values("_abs_dabs", ascending=False).head(4).copy()

            def _delta_str(v: float) -> str:
                try:
                    return f"{float(v):.2f}"
                except Exception:
                    return ""

            return pd.DataFrame(
                {
                    "Metric": df_top.get("index_label", df_top.get("index_slug", "")).astype(str),
                    "Δ Abs": df_top["_dabs"].map(_delta_str),
                }
            )

        for b in bundle_order:
            df_b = df_metrics[df_metrics["_bundle_disp"] == b].copy()
            if df_b.empty:
                continue

            # Divider page
            with irt_style_context(s):
                fig_div = plt.figure(figsize=(8.27, 11.69), dpi=s.fig_dpi)
                fig_div.patch.set_facecolor("white")
                fig_div.subplots_adjust(bottom=0.06)

                _draw_pdf_header(
                    fig=fig_div,
                    title=f"{b} — Bundle summary",
                    subtitle_lines=[
                        f"{district_name}, {state_name}",
                        f"Scenario: {scenario_disp}   |   Period: {sel_period}   |   Statistic: {stat_disp}",
                    ],
                    logo_path=logo_path,
                    is_cover=False,
                    show_logo=False,
                )

                total_b = int(len(df_b))
                baseline_b = int(pd.to_numeric(df_b.get("baseline", pd.Series([])), errors="coerce").notna().sum())
                vh_count = int((df_b.get("risk_class", pd.Series([])).astype(str).str.strip() == "Very High").sum())
                h_count = int((df_b.get("risk_class", pd.Series([])).astype(str).str.strip() == "High").sum())

                ax_kpi = fig_div.add_axes([0.06, 0.74, 0.88, 0.12])
                ax_kpi.axis("off")
                ax_kpi.text(0.0, 0.85, f"Metrics in this bundle: {total_b}", fontsize=12, fontweight="bold", ha="left", va="top")
                ax_kpi.text(0.0, 0.50, f"Very High risk: {vh_count}   |   High risk: {h_count}", fontsize=10, ha="left", va="top")
                ax_kpi.text(0.0, 0.20, f"Baseline available: {baseline_b}/{total_b}", fontsize=10, ha="left", va="top")

                gs = fig_div.add_gridspec(
                    nrows=2,
                    ncols=2,
                    left=0.06,
                    right=0.94,
                    bottom=0.14,
                    top=0.72,
                    wspace=0.22,
                    hspace=0.28,
                )
                ax_risk = fig_div.add_subplot(gs[0, 0])
                ax_top = fig_div.add_subplot(gs[0, 1])
                ax_delta = fig_div.add_subplot(gs[1, 0])
                ax_note = fig_div.add_subplot(gs[1, 1])
                ax_note.axis("off")

                _draw_table(ax_risk, _bundle_risk_counts(df_b), title="Risk distribution", font_size=8)
                _draw_table(
                    ax_top,
                    _bundle_top_risk(df_b),
                    title="Top risk drivers",
                    font_size=8,
                    wrap_cols={"Metric": 44},
                    col_widths=[0.78, 0.22],
                )

                top_d = _bundle_top_deltas(df_b)
                if top_d.empty:
                    ax_delta.axis("off")
                    ax_delta.text(0.0, 1.02, "Largest changes vs baseline", ha="left", va="bottom", fontsize=10, fontweight="bold")
                    ax_delta.text(0.0, 0.85, "No baseline deltas available.", ha="left", va="top", fontsize=9)
                else:
                    _draw_table(
                        ax_delta,
                        top_d,
                        title="Largest changes vs baseline (Δ Abs)",
                        font_size=8,
                        wrap_cols={"Metric": 44},
                        col_widths=[0.82, 0.18],
                    )

                ax_note.text(
                    0.0,
                    0.95,
                    "The following pages show each metric in this bundle with:\n"
                    "• Trend over time (historical + scenario)\n"
                    "• Scenario comparison (period means)\n"
                    "• Key numbers (current, baseline, Δ) and risk classification",
                    ha="left",
                    va="top",
                    fontsize=9,
                )

                _draw_pdf_footer(
                    fig=fig_div,
                    page_no=page_no,
                    total_pages=total_pages,
                    logo_path=logo_path,
                    show_logo=True,
                )
                pdf.savefig(fig_div)
                plt.close(fig_div)
                page_no += 1

            # Metric pages for this bundle
            for _, row_idx in df_b.iterrows():
                slug = str(row_idx.get("index_slug") or "").strip()
                if not slug:
                    continue

                idx_label = row_idx.get("index_label", slug)
                units = get_metric_display_units(
                    metric_slug=slug,
                    units=str(row_idx.get("units") or row_idx.get("unit") or "").strip(),
                )

                ts = ts_dict.get(slug, {}) or {}
                hist_ts = ts.get("historical", pd.DataFrame())
                scen_ts = ts.get("scenario", pd.DataFrame())

                panel_df = panel_dict.get(slug)

                with irt_style_context(s):
                    fig_idx = plt.figure(figsize=(8.27, 11.69), dpi=s.fig_dpi)
                    fig_idx.patch.set_facecolor("white")
                    fig_idx.subplots_adjust(top=0.98, bottom=0.06)

                    gs = fig_idx.add_gridspec(
                        nrows=3,
                        ncols=1,
                        height_ratios=[3.0, 2.0, 1.3],
                        hspace=0.25,
                        left=0.08,
                        right=0.92,
                        top=0.88,
                        bottom=0.10,
                    )

                    _draw_pdf_header(
                        fig=fig_idx,
                        title=f"{idx_label} — {district_name}",
                        subtitle_lines=[
                            f"Bundle: {b}",
                            f"Scenario: {scenario_disp}   |   Period: {sel_period}   |   Statistic: {stat_disp}",
                        ],
                        logo_path=logo_path,
                        is_cover=False,
                        show_logo=False,
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
                                render_context="pdf",
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
                                units=units,
                                render_context="pdf",
                            )
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

                    # 3) Narrative (kept compact; key numbers line is the main takeaway)
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

                    # Compact key numbers (avoid redundant bullet lists)
                    try:
                        u = (units or "").strip()

                        def _decimals_for_units(unit_str: str) -> int:
                            ul = (unit_str or "").strip().lower()
                            if not ul:
                                return 2
                            if "°" in unit_str or "deg" in ul or ul in {"c", "°c"}:
                                return 1
                            if "day" in ul or ul in {"days"}:
                                return 0
                            if "%" in unit_str:
                                return 1
                            return 2

                        def _fmt_num(val: object, unit_str: str, *, add_unit: bool = True) -> str:
                            if val is None or (isinstance(val, float) and pd.isna(val)):
                                return ""
                            if add_unit:
                                return format_metric_compact(val, metric_slug=slug, units=unit_str, na="")
                            return format_metric_compact(val, metric_slug=slug, units=unit_str, na="").replace(unit_str, "").strip()

                        current_s = _fmt_num(row_idx.get("current"), u, add_unit=True)
                        baseline_s = _fmt_num(row_idx.get("baseline"), u, add_unit=True)
                        delta_abs_s = _fmt_num(row_idx.get("delta_abs"), u, add_unit=True)

                        delta_pct = row_idx.get("delta_pct")
                        delta_pct_s = ""
                        if delta_pct is not None and not (isinstance(delta_pct, float) and pd.isna(delta_pct)):
                            try:
                                delta_pct_s = f"{float(delta_pct):.1f}%"
                            except Exception:
                                delta_pct_s = ""

                        baseline_period = str(row_idx.get("baseline_period") or "").strip().replace("_", "-")
                        risk = str(row_idx.get("risk_class") or "").strip()
                        rank = row_idx.get("rank_in_state")

                        parts: list[str] = []
                        if current_s:
                            parts.append(f"Current: {current_s}")
                        if baseline_s:
                            bp = baseline_period if baseline_period else "historical"
                            parts.append(f"Baseline ({bp}): {baseline_s}")
                        if delta_abs_s:
                            parts.append(f"Δ: {delta_abs_s}" + (f" ({delta_pct_s})" if delta_pct_s else ""))
                        if risk:
                            parts.append(f"Risk: {risk}")
                        if rank is not None and not (isinstance(rank, float) and pd.isna(rank)):
                            try:
                                parts.append(f"Rank: {int(round(float(rank)))}")
                            except Exception:
                                pass

                        key_line = " | ".join(parts).strip()
                        if key_line:
                            ax_text.text(
                                0.01,
                                y_text,
                                textwrap.fill(key_line, width=110),
                                fontsize=8,
                                va="top",
                                ha="left",
                                transform=ax_text.transAxes,
                            )
                            y_text -= 0.18
                    except Exception:
                        pass

                    _draw_pdf_footer(
                        fig=fig_idx,
                        page_no=page_no,
                        total_pages=total_pages,
                        logo_path=logo_path,
                        show_logo=True,
                    )

                    pdf.savefig(fig_idx)
                    plt.close(fig_idx)
                    page_no += 1

        # ------------------------------------------------------------------
        # Appendix A: Full metric summary table (may span multiple pages)
        # ------------------------------------------------------------------
        with irt_style_context(s):
            df_full = summary_df.copy()

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

            # Group for appendix pages: prefer bundle, else group.
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

            group_order = [g for g in pd.unique(df_full["_group_disp"]) if str(g).strip()] or ["All Metrics"]

            header_face = (0.95, 0.95, 0.95, 1.0)
            zebra_a = (1.0, 1.0, 1.0, 1.0)
            zebra_b = (0.98, 0.98, 0.98, 1.0)
            edge = (0.80, 0.80, 0.80, 1.0)

            for group_name in group_order:
                gdf = df_full[df_full["_group_disp"] == group_name].reset_index(drop=True)
                n_pages = max((len(gdf) + rows_per_page - 1) // rows_per_page, 1)

                for page_i in range(n_pages):
                    start = page_i * rows_per_page
                    end = min(start + rows_per_page, len(gdf))
                    chunk = gdf.iloc[start:end].copy()

                    # Build cell text (row-specific unit-aware formatting).
                    cell_text: list[list[str]] = []
                    for _, r in chunk.iterrows():
                        units = get_metric_display_units(
                            metric_slug=str(r.get("index_slug") or "").strip(),
                            units=str(r.get("units") or r.get("unit") or "").strip(),
                        )
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
                                row_cells.append(
                                    format_metric_compact(
                                        r.get(key),
                                        metric_slug=str(r.get("index_slug") or "").strip(),
                                        units=units,
                                        decimals=dec,
                                        na="",
                                    )
                                )
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

                    fig_app = plt.figure(figsize=(11.69, 8.27), dpi=s.fig_dpi)
                    fig_app.patch.set_facecolor("white")
                    fig_app.subplots_adjust(bottom=0.08)

                    ax_head = fig_app.add_axes([0.06, 0.88, 0.88, 0.10])
                    ax_head.axis("off")
                    ax_head.text(
                        0.0,
                        0.85,
                        "Appendix A — Full metric summary table",
                        ha="left",
                        va="top",
                        fontsize=14,
                        fontweight="bold",
                    )
                    ax_head.text(
                        0.0,
                        0.35,
                        f"Section: {group_name}  (page {page_i + 1} of {n_pages})",
                        ha="left",
                        va="top",
                        fontsize=10,
                    )

                    ax_tbl = fig_app.add_axes([0.06, 0.10, 0.88, 0.76])
                    ax_tbl.axis("off")

                    table = ax_tbl.table(
                        cellText=cell_text,
                        colLabels=col_labels,
                        colWidths=col_widths,
                        cellLoc="left",
                        colLoc="left",
                        bbox=[0.0, 0.0, 1.0, 1.0],
                    )
                    table.auto_set_font_size(False)
                    table.set_fontsize(8)

                    for (ri, ci), cell in table.get_celld().items():
                        cell.set_edgecolor(edge)
                        cell.set_linewidth(0.6)
                        cell.PAD = 0.02
                        if ri == 0:
                            cell.set_facecolor(header_face)
                            cell.get_text().set_fontweight("bold")
                            cell.get_text().set_ha("left")
                        else:
                            cell.set_facecolor(zebra_a if (ri % 2 == 0) else zebra_b)

                            ha = col_specs[ci].get("ha", "left") if ci < len(col_specs) else "left"
                            cell.get_text().set_ha(ha)

                    _draw_pdf_footer(
                        fig=fig_app,
                        page_no=page_no,
                        total_pages=total_pages,
                        logo_path=logo_path,
                        show_logo=True,
                        left_text="India Resilience Tool — Climate profile (Appendix A)",
                    )

                    pdf.savefig(fig_app)
                    plt.close(fig_app)
                    page_no += 1

    buf.seek(0)
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

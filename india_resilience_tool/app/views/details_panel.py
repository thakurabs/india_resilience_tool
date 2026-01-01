"""
Single-district details panel (right column) for IRT.

This module extracts the single-district right-column UI from
dashboard_unfactored_impl.py. It includes:
- Risk summary (current value, change vs baseline, position in state)
- Trend over time (historical + scenario + narrative)
- Scenario comparison (period-mean bar chart)
- Detailed statistics + PDF generation
- Case-study export (multi-index PDF/ZIP)
- District comparison

Widget keys preserved:
- compare_district
- case_study_indices
- btn_build_case_study
- download_case_study_pdf
- download_case_study_zip
- btn_district_pdf_*
- btn_dist_pdf_dl

Session state keys used:
- case_study_summary
- case_study_ts
- case_study_panels
- district_pdf_path_*

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

import pandas as pd


def render_risk_summary(
    *,
    current_val_f: Optional[float],
    baseline_val_f: Optional[float],
    baseline_col: Optional[str],
    rank_in_state: Optional[int],
    n_in_state: Optional[int],
    percentile_state: Optional[float],
    variable_label: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    state_to_show: str,
    level: str = "district",
    parent_district_name: Optional[str] = None,
    rank_in_district: Optional[int] = None,
    n_in_district: Optional[int] = None,
    percentile_district: Optional[float] = None,
) -> None:
    """
    Render the Risk summary expander.

    District mode:
        - Current value
        - Change vs baseline
        - Position in state

    Block mode:
        - Current value
        - Change vs baseline
        - Position in district
        - Position in state
    """
    import streamlit as st

    level_norm = str(level).strip().lower()
    is_block = level_norm == "block"

    with st.expander("Risk summary", expanded=True):
        cols = st.columns(4) if is_block else st.columns(3)

        # --- Current value ---
        with cols[0]:
            st.markdown("**Current value**")
            if current_val_f is not None:
                st.metric(
                    label="Current Value",
                    label_visibility="collapsed",
                    value=f"{current_val_f:.2f}",
                    help=f"{variable_label} ({sel_scenario}, {sel_period}, {sel_stat})",
                )
            else:
                st.write("No data")

        # --- Change vs baseline ---
        with cols[1]:
            st.markdown("**Change vs baseline**")
            if current_val_f is not None and baseline_val_f is not None:
                diff_abs = current_val_f - baseline_val_f
                diff_pct = (
                    (diff_abs / baseline_val_f * 100.0)
                    if baseline_val_f not in (0.0, None)
                    else None
                )
                delta_str = f"{diff_abs:+.2f}"
                if diff_pct is not None:
                    delta_str += f" ({diff_pct:+.1f}%)"

                if baseline_col:
                    parts = str(baseline_col).split("__")
                    if len(parts) == 4:
                        _, base_scenario, base_period, base_stat = parts
                        baseline_desc = f"{base_scenario}, {base_period}, {base_stat}"
                    else:
                        baseline_desc = str(baseline_col)
                else:
                    baseline_desc = "not found"

                st.metric(
                    label="Change Vs Baseline",
                    label_visibility="collapsed",
                    value=f"{baseline_val_f:.2f}",
                    delta=delta_str,
                    help=f"Baseline: {baseline_desc}",
                )
            else:
                st.write("Baseline not available")

        # --- Position in district (block mode only) ---
        if is_block:
            with cols[2]:
                st.markdown("**Position in district**")
                if rank_in_district is not None and n_in_district is not None:
                    rank_label = f"{rank_in_district}/{n_in_district}"
                    district_label = parent_district_name or "selected district"
                    if percentile_district is not None:
                        help_text = (
                            f"Approximate percentile: {percentile_district:.0f}th\n"
                            f"Computed among {n_in_district} blocks in {district_label} "
                            f"for this index (higher values = higher rank)."
                        )
                    else:
                        help_text = (
                            f"Computed among {n_in_district} blocks in {district_label} "
                            f"(higher values = higher rank)."
                        )

                    st.metric(
                        label="Rank in district",
                        label_visibility="collapsed",
                        value=rank_label,
                        help=help_text,
                    )
                else:
                    st.write("Insufficient data")

            pos_state_col = cols[3]
        else:
            pos_state_col = cols[2]

        # --- Position in state ---
        with pos_state_col:
            st.markdown("**Position in state**")
            if rank_in_state is not None and n_in_state is not None:
                rank_label = f"{rank_in_state}/{n_in_state}"
                unit_word = "blocks" if is_block else "districts"
                if percentile_state is not None:
                    help_text = (
                        f"Approximate percentile: {percentile_state:.0f}th\n"
                        f"Computed among {n_in_state} {unit_word} in {state_to_show} "
                        f"for this index (higher values = higher rank)."
                    )
                else:
                    help_text = (
                        f"Computed among {n_in_state} {unit_word} in {state_to_show} "
                        f"(higher values = higher rank)."
                    )

                st.metric(
                    label="Rank in state",
                    label_visibility="collapsed",
                    value=rank_label,
                    help=help_text,
                )
            else:
                st.write("Insufficient data")


def render_trend_over_time(
    *,
    hist_ts: pd.DataFrame,
    scen_ts: pd.DataFrame,
    variable_label: str,
    sel_scenario: str,
    district_name: str,
    state_dir_for_fs: str,
    district_for_fs: str,
    fig_size_panel: tuple[float, float],
    fig_dpi_panel: int,
    font_size_legend: int,
    logo_path: Optional[Path] = None,
    create_trend_figure_fn: Callable[..., Any],
) -> None:
    """Render the Trend over time expander with sparkline + narrative."""
    import streamlit as st

    with st.expander("Trend over time", expanded=False):
        st.caption(
            f"Looking for yearly CSVs under: {state_dir_for_fs} / {district_for_fs} "
            f"(historical + {sel_scenario})"
        )

        if not hist_ts.empty or not scen_ts.empty:
            st.markdown("**Trend over time**")

            try:
                fig_ts = create_trend_figure_fn(
                    hist_ts=hist_ts,
                    scen_ts=scen_ts,
                    idx_label=variable_label,
                    scenario_name=sel_scenario,
                    figsize=fig_size_panel,
                    fig_dpi=fig_dpi_panel,
                    font_size_legend=font_size_legend,
                    logo_path=logo_path,
                )
            except TypeError:
                # Backward-compatible fallback if chart fn doesn't accept the new kwargs
                fig_ts = create_trend_figure_fn(
                    hist_ts=hist_ts,
                    scen_ts=scen_ts,
                    idx_label=variable_label,
                    scenario_name=sel_scenario,
                )
                try:
                    from india_resilience_tool.viz.style import add_ra_logo

                    add_ra_logo(fig_ts, logo_path)
                except Exception:
                    pass

            st.pyplot(fig_ts, use_container_width=True)

            # Narrative
            try:
                parts = []
                if not hist_ts.empty:
                    parts.append(hist_ts[["year", "mean"]])
                if not scen_ts.empty:
                    parts.append(scen_ts[["year", "mean"]])
                if parts:
                    combined = pd.concat(parts, ignore_index=True).sort_values("year")
                    start_year = int(combined["year"].iloc[0])
                    end_year = int(combined["year"].iloc[-1])
                    start_val = float(combined["mean"].iloc[0])
                    end_val = float(combined["mean"].iloc[-1])
                    delta = end_val - start_val
                    pct = (delta / start_val * 100.0) if start_val not in (0.0, None) else None

                    if abs(delta) < 0.1:
                        trend_word = "has remained broadly stable"
                    elif delta > 0:
                        trend_word = "has increased"
                    else:
                        trend_word = "has decreased"

                    if pct is not None:
                        st.markdown(
                            f"**Narrative:** Between **{start_year}** and **{end_year}**, "
                            f"{variable_label.lower()} in **{district_name}** "
                            f"{trend_word}, from about **{start_val:.1f}** to **{end_val:.1f}** "
                            f"({pct:+.1f}% change)."
                        )
                    else:
                        st.markdown(
                            f"**Narrative:** Between **{start_year}** and **{end_year}**, "
                            f"{variable_label.lower()} in **{district_name}** "
                            f"{trend_word}."
                        )
            except Exception:
                pass
        else:
            st.caption("No yearly time-series available for this district (historical or scenario).")


def render_scenario_comparison(
    *,
    row: pd.Series,
    schema_items: Sequence[Mapping[str, Any]],
    sel_metric: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    variable_label: str,
    district_name: str,
    fig_size_panel: tuple[float, float],
    fig_dpi_panel: int,
    font_size_title: int,
    font_size_label: int,
    font_size_ticks: int,
    font_size_legend: int,
    logo_path: Optional[Path] = None,
    period_order: Sequence[str],
    scenario_display: Mapping[str, str],
    build_scenario_panel_fn: Callable[..., pd.DataFrame],
    make_scenario_figure_fn: Callable[..., tuple[Any, Any]],
) -> None:
    """Render the Scenario comparison (period-mean) expander."""
    import streamlit as st

    with st.expander("Scenario comparison (period-mean)", expanded=False):
        panel_df = build_scenario_panel_fn(
            row=row,
            schema_items=schema_items,
            metric_name=sel_metric,
            sel_stat=sel_stat,
        )

        if panel_df is not None and not panel_df.empty:
            try:
                fig_sc, ax_sc = make_scenario_figure_fn(
                    panel_df=panel_df,
                    metric_label=variable_label,
                    sel_scenario=sel_scenario,
                    sel_period=sel_period,
                    sel_stat=sel_stat,
                    district_name=district_name,
                    figsize=fig_size_panel,
                    fig_dpi=fig_dpi_panel,
                    font_size_title=font_size_title,
                    font_size_label=font_size_label,
                    font_size_ticks=font_size_ticks,
                    font_size_legend=font_size_legend,
                    logo_path=logo_path,
                )
            except TypeError:
                fig_sc, ax_sc = make_scenario_figure_fn(
                    panel_df=panel_df,
                    metric_label=variable_label,
                    sel_scenario=sel_scenario,
                    sel_period=sel_period,
                    sel_stat=sel_stat,
                    district_name=district_name,
                    figsize=fig_size_panel,
                    fig_dpi=fig_dpi_panel,
                    font_size_title=font_size_title,
                    font_size_label=font_size_label,
                    font_size_ticks=font_size_ticks,
                    font_size_legend=font_size_legend,
                )
                try:
                    from india_resilience_tool.viz.style import add_ra_logo

                    add_ra_logo(fig_sc, logo_path)
                except Exception:
                    pass

            if fig_sc is not None:
                st.pyplot(fig_sc, use_container_width=True)

            # Numeric summary in text
            lines = []
            for period in period_order:
                sub = panel_df[panel_df["period"] == period]
                if sub.empty:
                    continue
                parts = []
                for scen in ["historical", "ssp245", "ssp585"]:
                    sub_s = sub[sub["scenario"] == scen]
                    if sub_s.empty:
                        continue
                    val = sub_s["value"].iloc[0]
                    parts.append(f"{scenario_display.get(scen, scen)} = {val:.1f}")
                if parts:
                    lines.append(f"- **{period}**: " + ", ".join(parts))

            if lines:
                st.markdown(
                    "For this district and selected statistic, the **period-average** values are:\n"
                    + "\n".join(lines)
                )
        else:
            st.caption(
                "Scenario comparison (period-mean) not available for this district/index combination."
            )


def render_detailed_statistics(
    *,
    row: pd.Series,
    sel_metric: str,
    sel_scenario: str,
    sel_period: str,
    variable_slug: str,
    variable_label: str,
    state_to_show: str,
    selected_district: str,
    district_yearly_scen: pd.DataFrame,
    out_dir: Path,
    logo_path: Optional[Path] = None,
    make_district_yearly_pdf_fn: Callable[..., Optional[Path]],
) -> None:
    """Render the Detailed statistics expander with optional PDF generation."""
    import streamlit as st

    with st.expander("Detailed statistics for selected district", expanded=False):
        # Basic stats table
        stats_list = ["mean", "median", "p05", "p95", "std"]
        rows_stats = []
        for sname in stats_list:
            coln = f"{sel_metric}__{sel_scenario}__{sel_period}__{sname}"
            val = row.get(coln)
            rows_stats.append({"Statistic": sname, "Value": val})

        df_stats_state = pd.DataFrame(rows_stats)
        df_stats_state["Value"] = pd.to_numeric(df_stats_state["Value"], errors="coerce")
        st.table(df_stats_state.set_index("Statistic"))

        # PDF generation
        st.caption(
            "You can optionally generate a PDF of the district's yearly "
            "time-series for the selected scenario."
        )

        pdf_state_key = (
            f"district_pdf_path_{variable_slug}_{state_to_show}_{selected_district}_{sel_scenario}"
        )
        pdf_path_d = st.session_state.get(pdf_state_key)

        if st.button(
            "Generate district yearly time-series PDF",
            key=f"btn_district_pdf_{variable_slug}_{state_to_show}_{selected_district}_{sel_scenario}",
        ):
            pdf_path_d = make_district_yearly_pdf_fn(
                df_yearly=district_yearly_scen,
                state_name=state_to_show,
                district_name=row.get("district_name", selected_district),
                scenario_name=sel_scenario,
                metric_label=variable_label,
                out_dir=out_dir,
                logo_path=logo_path,
            )
            if pdf_path_d and pdf_path_d.exists():
                st.session_state[pdf_state_key] = pdf_path_d
            else:
                st.session_state.pop(pdf_state_key, None)
                pdf_path_d = None

        if pdf_path_d and pdf_path_d.exists():
            with open(pdf_path_d, "rb") as fh:
                st.download_button(
                    "⬇️ Download district yearly time-series (PDF)",
                    fh.read(),
                    file_name=pdf_path_d.name,
                    mime="application/pdf",
                    key="btn_dist_pdf_dl",
                )

            abs_url_d = pdf_path_d.resolve().as_uri()
            st.markdown(
                f'<a href="{abs_url_d}" target="_blank" rel="noopener">'
                f"🗎 Open district yearly figure in a new tab</a>",
                unsafe_allow_html=True,
            )
        else:
            st.caption(
                "No yearly time-series PDF is currently available for this "
                "district/scenario. Click the button above to generate it."
            )


def render_case_study_export(
    *,
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    state_to_show: str,
    district_name: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    logo_path: Optional[Path] = None,
    build_case_study_data_fn: Callable[..., tuple[pd.DataFrame, dict, dict]],
    make_case_study_pdf_fn: Callable[..., bytes],
    make_case_study_zip_fn: Callable[..., bytes],
    slugify_fs_fn: Callable[[str], str],
) -> None:
    """Render the Case study export expander (single district, multi-index)."""
    import streamlit as st

    with st.expander("📄 Case study export (single district, multi-index)", expanded=False):
        st.caption(
            "Build a case-study style report for the selected district across "
            "multiple climate indices (experimental)."
        )

        index_options = list(variables.keys())
        default_indices = (
            [variable_slug] if variable_slug in index_options else index_options[:1]
        )
        selected_index_slugs = st.multiselect(
            "Indices to include in the report",
            options=index_options,
            default=default_indices,
            format_func=lambda s: variables[s]["label"],
            key="case_study_indices",
        )

        if not selected_index_slugs:
            st.info("Select at least one index to build the case-study report.")
        else:
            if st.button(
                "Build case-study data for this district",
                key="btn_build_case_study",
            ):
                with st.spinner("Assembling climate profile for selected indices..."):
                    summary_df_cs, ts_dict_cs, panel_dict_cs = build_case_study_data_fn(
                        state_name=state_to_show,
                        district_name=district_name,
                        index_slugs=selected_index_slugs,
                        sel_scenario=sel_scenario,
                        sel_period=sel_period,
                        sel_stat=sel_stat,
                    )
                    if summary_df_cs is None or summary_df_cs.empty:
                        st.warning(
                            "No data found for the selected index/district combination. "
                            "Try including fewer indices or a different scenario/period/statistic."
                        )
                    else:
                        st.session_state["case_study_summary"] = summary_df_cs
                        st.session_state["case_study_ts"] = ts_dict_cs
                        st.session_state["case_study_panels"] = panel_dict_cs

            summary_df_cs = st.session_state.get("case_study_summary")
            ts_dict_cs = st.session_state.get("case_study_ts")
            panel_dict_cs = st.session_state.get("case_study_panels")

            if isinstance(summary_df_cs, pd.DataFrame) and not summary_df_cs.empty:
                st.markdown("**Preview of case-study summary table**")
                st.dataframe(summary_df_cs)

                try:
                    pdf_bytes = make_case_study_pdf_fn(
                        state_name=state_to_show,
                        district_name=district_name,
                        summary_df=summary_df_cs,
                        ts_dict=ts_dict_cs or {},
                        panel_dict=panel_dict_cs or {},
                        sel_scenario=sel_scenario,
                        sel_period=sel_period,
                        sel_stat=sel_stat,
                        logo_path=logo_path,
                    )
                except TypeError:
                    pdf_bytes = make_case_study_pdf_fn(
                        state_name=state_to_show,
                        district_name=district_name,
                        summary_df=summary_df_cs,
                        ts_dict=ts_dict_cs or {},
                        panel_dict=panel_dict_cs or {},
                        sel_scenario=sel_scenario,
                        sel_period=sel_period,
                        sel_stat=sel_stat,
                    )

                if pdf_bytes:
                    safe_state = slugify_fs_fn(state_to_show)
                    safe_dist = slugify_fs_fn(district_name)
                    pdf_filename = f"climate_profile_{safe_state}__{safe_dist}.pdf"

                    st.download_button(
                        label="⬇️ Download case-study PDF",
                        data=pdf_bytes,
                        file_name=pdf_filename,
                        mime="application/pdf",
                        key="download_case_study_pdf",
                    )

                    zip_bytes = make_case_study_zip_fn(
                        state_name=state_to_show,
                        district_name=district_name,
                        summary_df=summary_df_cs,
                        ts_dict=ts_dict_cs or {},
                        panel_dict=panel_dict_cs or {},
                        pdf_bytes=pdf_bytes,
                    )
                    st.download_button(
                        label="⬇️ Download PDF + CSVs as ZIP",
                        data=zip_bytes,
                        file_name=f"climate_profile_{safe_state}__{safe_dist}__with_data.zip",
                        mime="application/zip",
                        key="download_case_study_zip",
                    )
            else:
                st.caption(
                    "Build the case-study data using the button above to enable downloads."
                )


def render_district_comparison(
    *,
    merged: Any,  # GeoDataFrame
    state_to_show: str,
    district_name: str,
    metric_col: str,
    current_val_f: Optional[float],
    variable_label: str,
    sel_stat: str,
    fig_size_mini: tuple[float, float],
    fig_dpi_panel: int,
    font_size_title: int,
    font_size_label: int,
    font_size_ticks: int,
    logo_path: Optional[Path] = None,
) -> None:
    """Render the Compare with another district expander."""
    import matplotlib.pyplot as plt
    import streamlit as st
    from india_resilience_tool.viz.style import add_ra_logo, strip_spines

    with st.expander("Compare with another district", expanded=False):
        same_state_mask = (
            merged["state_name"].astype(str).str.strip().str.lower()
            == str(state_to_show).strip().lower()
        )
        compare_candidates = (
            merged.loc[same_state_mask, "district_name"]
            .astype(str)
            .sort_values()
            .unique()
            .tolist()
        )
        compare_candidates = [d for d in compare_candidates if d != district_name]

        if compare_candidates:
            comp_choice = st.selectbox(
                "Compare with",
                options=["(None)"] + compare_candidates,
                index=0,
                key="compare_district",
            )

            if comp_choice != "(None)":
                mask_c = (
                    merged["district_name"].astype(str).str.strip()
                    == str(comp_choice).strip()
                )
                comp_row = merged[mask_c].iloc[0] if mask_c.any() else None

                if comp_row is not None:
                    val_this = current_val_f
                    val_other = comp_row.get(metric_col)
                    val_other_f = float(val_other) if not pd.isna(val_other) else None

                    if (val_this is not None) and (val_other_f is not None):
                        diff = val_this - val_other_f
                        direction = (
                            "higher than"
                            if diff > 0
                            else "lower than"
                            if diff < 0
                            else "the same as"
                        )
                        st.markdown(
                            f"- **{variable_label}** in **{district_name}** "
                            f"is **{abs(diff):.2f}** {direction} in **{comp_choice}** "
                            f"for the selected scenario and period."
                        )

                        # Small visual comparison: two bars side by side
                        fig_cmp, ax_cmp = plt.subplots(figsize=fig_size_mini, dpi=fig_dpi_panel)
                        labels_cmp = [district_name, comp_choice]
                        values_cmp = [val_this, val_other_f]

                        colors_cmp = ["tab:blue", "tab:grey"]
                        bars = ax_cmp.bar(labels_cmp, values_cmp, color=colors_cmp)

                        ax_cmp.set_ylabel(f"{variable_label} ({sel_stat})", fontsize=font_size_label)
                        ax_cmp.set_title("District comparison", fontsize=font_size_title)
                        ax_cmp.tick_params(axis="x", labelsize=font_size_ticks)
                        ax_cmp.tick_params(axis="y", labelsize=font_size_ticks)
                        ax_cmp.grid(True, axis="y", linestyle="--", alpha=0.25)

                        # Annotate values on top of bars
                        for b in bars:
                            height = b.get_height()
                            ax_cmp.text(
                                b.get_x() + b.get_width() / 2,
                                height,
                                f"{height:.1f}",
                                ha="center",
                                va="bottom",
                                fontsize=8,
                            )

                        strip_spines(ax_cmp)

                        try:
                            fig_cmp.tight_layout()
                        except Exception:
                            pass

                        add_ra_logo(fig_cmp, logo_path, width_frac=0.14, pad_frac=0.012, alpha=0.95)
                        st.pyplot(fig_cmp, use_container_width=True)

                    else:
                        st.caption(
                            "Comparison data not fully available for the selected index."
                        )
        else:
            st.caption("No other districts found in this state for comparison.")


def render_details_panel(
    *,
    # Core district/state context
    row: pd.Series,
    district_name: str,
    state_to_show: str,
    selected_district: str,
    # Metric / variable context
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    metric_col: str,
    sel_metric: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    # Risk summary data
    current_val_f: Optional[float],
    baseline_val_f: Optional[float],
    baseline_col: Optional[str],
    rank_in_state: Optional[int],
    n_in_state: Optional[int],
    percentile_state: Optional[float],
    # Time series data
    hist_ts: pd.DataFrame,
    scen_ts: pd.DataFrame,
    district_yearly_scen: pd.DataFrame,
    # Schema for scenario comparison
    schema_items: Sequence[Mapping[str, Any]],
    # GeoDataFrame for district comparison
    merged: Any,
    # Figure styling
    fig_size_panel: tuple[float, float],
    fig_dpi_panel: int,
    font_size_title: int,
    font_size_label: int,
    font_size_ticks: int,
    font_size_legend: int,
    # Constants
    period_order: Sequence[str],
    scenario_display: Mapping[str, str],
    out_dir: Path,
    # Callable dependencies (injected for testability)
    create_trend_figure_fn: Callable[..., Any],
    build_scenario_panel_fn: Callable[..., pd.DataFrame],
    make_scenario_figure_fn: Callable[..., tuple[Any, Any]],
    make_district_yearly_pdf_fn: Callable[..., Optional[Path]],
    build_case_study_data_fn: Callable[..., tuple[pd.DataFrame, dict, dict]],
    make_case_study_pdf_fn: Callable[..., bytes],
    make_case_study_zip_fn: Callable[..., bytes],
    slugify_fs_fn: Callable[[str], str],
    # Optional: filesystem paths for caption
    state_dir_for_fs: Optional[str] = None,
    district_for_fs: Optional[str] = None,
    # Optional: Resilience Actions logo
    logo_path: Optional[Path] = None,
    # Block-level support
    level: str = "district",
    block_name: Optional[str] = None,
    parent_district_name: Optional[str] = None,
    rank_in_district: Optional[int] = None,
    n_in_district: Optional[int] = None,
    percentile_district: Optional[float] = None,
) -> None:

    """
    Render the complete single-district details panel (right column).

    This is the main entry point for the details panel, composing all sub-renderers.

    Contract:
    - Preserves all widget keys from the legacy dashboard
    - Preserves all session_state keys
    - Takes explicit inputs rather than relying on globals
    """
    import streamlit as st

    variable_label = variables.get(variable_slug, {}).get("label", variable_slug)

    level_norm = str(level).strip().lower()
    is_block = level_norm == "block"

    # Header (block-aware)
    if is_block:
        unit_name = (
            block_name
            or (str(row.get("block_name")).strip() if "block_name" in row else "")
            or district_name
        )
        parent_dist = (
            parent_district_name
            or (str(row.get("district_name")).strip() if "district_name" in row else "")
            or selected_district
        )
        st.subheader(f"{unit_name} (Block)")
        st.caption(f"District: {parent_dist} | State: {state_to_show}")
    else:
        st.subheader(f"{district_name} (District)")
        st.caption(f"State: {state_to_show}")

    # Normalize panel figure size to 16:9 (dashboard style contract)
    fig_size_panel_169 = fig_size_panel
    try:
        from india_resilience_tool.viz.style import ensure_16x9_figsize

        fig_size_panel_169 = ensure_16x9_figsize(fig_size_panel, mode="fit_width")
    except Exception:
        try:
            w = float(fig_size_panel[0])
            fig_size_panel_169 = (w, w * (9.0 / 16.0))
        except Exception:
            fig_size_panel_169 = fig_size_panel

    # Mini figures (also 16:9), slightly smaller than the main panel
    try:
        w_mini = float(fig_size_panel_169[0]) * 0.85
        fig_size_mini_169 = (w_mini, w_mini * (9.0 / 16.0))
    except Exception:
        fig_size_mini_169 = fig_size_panel_169

    # 1. Risk summary
    render_risk_summary(
        current_val_f=current_val_f,
        baseline_val_f=baseline_val_f,
        baseline_col=baseline_col,
        rank_in_state=rank_in_state,
        n_in_state=n_in_state,
        percentile_state=percentile_state,
        variable_label=variable_label,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        sel_stat=sel_stat,
        state_to_show=state_to_show,
        level=level,
        parent_district_name=parent_district_name,
        rank_in_district=rank_in_district,
        n_in_district=n_in_district,
        percentile_district=percentile_district,
    )

    # 2. Trend over time
    render_trend_over_time(
        hist_ts=hist_ts,
        scen_ts=scen_ts,
        variable_label=variable_label,
        sel_scenario=sel_scenario,
        district_name=district_name,
        state_dir_for_fs=state_dir_for_fs or state_to_show,
        district_for_fs=district_for_fs or district_name,
        fig_size_panel=fig_size_panel_169,
        fig_dpi_panel=fig_dpi_panel,
        font_size_legend=font_size_legend,
        logo_path=logo_path,
        create_trend_figure_fn=create_trend_figure_fn,
    )

    # 3. Scenario comparison
    render_scenario_comparison(
        row=row,
        schema_items=schema_items,
        sel_metric=sel_metric,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        sel_stat=sel_stat,
        variable_label=variable_label,
        district_name=district_name,
        fig_size_panel=fig_size_panel_169,
        fig_dpi_panel=fig_dpi_panel,
        font_size_title=font_size_title,
        font_size_label=font_size_label,
        font_size_ticks=font_size_ticks,
        font_size_legend=font_size_legend,
        logo_path=logo_path,
        period_order=period_order,
        scenario_display=scenario_display,
        build_scenario_panel_fn=build_scenario_panel_fn,
        make_scenario_figure_fn=make_scenario_figure_fn,
    )

    # 4. Detailed statistics + PDF generation
    render_detailed_statistics(
        row=row,
        sel_metric=sel_metric,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        variable_slug=variable_slug,
        variable_label=variable_label,
        state_to_show=state_to_show,
        selected_district=selected_district,
        district_yearly_scen=district_yearly_scen,
        out_dir=out_dir,
        logo_path=logo_path,
        make_district_yearly_pdf_fn=make_district_yearly_pdf_fn,
    )

    # 5. Case study export (multi-index)
    render_case_study_export(
        variables=variables,
        variable_slug=variable_slug,
        state_to_show=state_to_show,
        district_name=district_name,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        sel_stat=sel_stat,
        logo_path=logo_path,
        build_case_study_data_fn=build_case_study_data_fn,
        make_case_study_pdf_fn=make_case_study_pdf_fn,
        make_case_study_zip_fn=make_case_study_zip_fn,
        slugify_fs_fn=slugify_fs_fn,
    )

    # 6. District comparison (district-mode only)
    if str(level).strip().lower() != "block":
        render_district_comparison(
            merged=merged,
            state_to_show=state_to_show,
            district_name=district_name,
            metric_col=metric_col,
            current_val_f=current_val_f,
            variable_label=variable_label,
            sel_stat=sel_stat,
            fig_size_mini=fig_size_mini_169,
            fig_dpi_panel=fig_dpi_panel,
            font_size_title=font_size_title,
            font_size_label=font_size_label,
            font_size_ticks=font_size_ticks,
            logo_path=logo_path,
        )
    else:
        import streamlit as st

        with st.expander("District comparison", expanded=False):
            st.caption("District comparison is not available in block mode yet.")

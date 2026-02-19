"""
Single-unit details panel (right column) for IRT.

This module extracts the right-column "Climate Profile" UI from the legacy dashboard.
It includes:
- Risk summary (current value, change vs baseline, position in state/district)
- Trend over time (historical + scenario)
- Scenario comparison (period-mean bar chart)
- Case-study export (multi-index PDF/ZIP)

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""


from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

import html
import pandas as pd

from india_resilience_tool.app.state import VIEW_RANKINGS
from india_resilience_tool.viz.formatting import format_delta, format_percent, format_value


_RISK_SUMMARY_CSS = """
<style>
/* ---------------------------
   Risk summary stat cards
   (big number + small units)
---------------------------- */
.irt-risk-metric {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  padding-top: 0.05rem;
}

.irt-risk-value {
  display: flex;
  align-items: baseline;
  gap: 0.35rem;
  line-height: 1;
  white-space: nowrap;
}

.irt-risk-number {
  font-weight: 700;
  letter-spacing: -0.02em;
  font-size: clamp(1.55rem, 1.2vw + 1.05rem, 2.35rem);
}

.irt-risk-unit {
  font-weight: 500;
  opacity: 0.6;
  font-size: clamp(0.75rem, 0.45vw + 0.6rem, 0.95rem);
}

/* Delta badge (keeps the “pill” feel) */
.irt-risk-delta {
  display: flex;
  justify-content: flex-start;
}

.irt-delta-badge {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.18rem 0.5rem;
  font-size: 0.8rem;
  font-weight: 600;
  line-height: 1;
}

.irt-delta-pos { background: rgba(34,197,94,0.12); color: rgb(34,197,94); }
.irt-delta-neg { background: rgba(239,68,68,0.12); color: rgb(239,68,68); }
.irt-delta-neutral { background: rgba(148,163,184,0.18); color: rgba(148,163,184,1); }

/* ---------------------------
   Tiny icon button for "Open rankings table"
   (scoped via title)
---------------------------- */
button[title="Open rankings table"] {
  background: transparent !important;
  border: 1px solid rgba(148,163,184,0.35) !important;
  padding: 0.05rem 0.35rem !important;
  min-height: 1.55rem !important;
  height: 1.55rem !important;
  line-height: 1 !important;
  border-radius: 0.45rem !important;
  color: rgba(100,116,139,1) !important;
  font-weight: 800 !important;
  font-size: 0.9rem !important;
}

button[title="Open rankings table"]:hover {
  border-color: rgba(148,163,184,0.65) !important;
  color: var(--primary-color) !important;
}

button[title="Open rankings table"]:disabled {
  opacity: 0.45 !important;
}
</style>
"""


def _inject_risk_summary_css() -> None:
    """
    Inject Risk Summary CSS.

    Streamlit reruns rebuild the UI DOM on every interaction (e.g., switching Map ↔ Rankings).
    To keep typography consistent across views, inject the CSS on every run.
    """
    import streamlit as st
    st.markdown(_RISK_SUMMARY_CSS, unsafe_allow_html=True)


def _risk_value_html(*, number_str: str, units: Optional[str]) -> str:
    """Return HTML for a big number with small inline units."""
    number_esc = html.escape(str(number_str))
    unit = (units or "").strip()
    unit_html = (
        f'<span class="irt-risk-unit">{html.escape(unit)}</span>'
        if unit
        else ""
    )
    return (
        f'<div class="irt-risk-value">'
        f'<span class="irt-risk-number">{number_esc}</span>'
        f'{unit_html}'
        f"</div>"
    )


def _risk_metric_html(
    *,
    number_str: str,
    units: Optional[str],
    delta_text: Optional[str] = None,
    delta_kind: str = "neutral",
    help_text: Optional[str] = None,
) -> str:
    """Return a compact metric block HTML with optional delta badge."""
    title_attr = f' title="{html.escape(help_text)}"' if help_text else ""
    value_html = _risk_value_html(number_str=number_str, units=units)

    delta_html = ""
    if delta_text:
        delta_html = (
            f'<div class="irt-risk-delta">'
            f'<span class="irt-delta-badge irt-delta-{html.escape(delta_kind)}">'
            f"{html.escape(delta_text)}"
            f"</span></div>"
        )

    return f'<div class="irt-risk-metric"{title_attr}>{value_html}{delta_html}</div>'


def render_risk_summary(
    *,
    current_val_f: Optional[float],
    baseline_val_f: Optional[float],
    baseline_col: Optional[str],
    rank_in_state: Optional[int],
    n_in_state: Optional[int],
    percentile_state: Optional[float],
    rank_higher_is_worse: bool = True,
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
    units: Optional[str] = None,
) -> None:
    """
    Render the Risk summary expander.

    District mode (left → right):
        - Historical baseline
        - Current value (with delta pill vs historical baseline)
        - Position in state

    Block mode (left → right):
        - Historical baseline
        - Current value (with delta pill vs historical baseline)
        - Position in district
        - Position in state
    """
    import streamlit as st

    _inject_risk_summary_css()

    level_norm = str(level).strip().lower()
    is_block = level_norm == "block"

    # Baseline descriptor for tooltip/help
    if baseline_col:
        parts = str(baseline_col).split("__")
        if len(parts) == 4:
            _, base_scenario, base_period, base_stat = parts
            baseline_desc = f"{base_scenario}, {base_period}, {base_stat}"
        else:
            baseline_desc = str(baseline_col)
    else:
        baseline_desc = "metadata not available"

    # Delta is conceptually attached to CURRENT value (vs historical baseline)
    delta_str: Optional[str] = None
    delta_kind = "neutral"
    if current_val_f is not None and baseline_val_f is not None:
        diff_abs = current_val_f - baseline_val_f
        diff_pct = (
            (diff_abs / baseline_val_f * 100.0)
            if baseline_val_f not in (0.0, None)
            else None
        )
        delta_str = format_delta(diff_abs, units=units)
        if diff_pct is not None:
            delta_str += f" ({format_percent(diff_pct, decimals=1, show_sign=True)})"
        delta_kind = "pos" if diff_abs > 0 else ("neg" if diff_abs < 0 else "neutral")

    with st.expander("Risk summary", expanded=True):
        cols = st.columns(4) if is_block else st.columns(3)

        # Column mapping (keeps "Position in state" at the far right)
        col_baseline = cols[0]
        col_current = cols[1]
        col_pos_district = cols[2] if is_block else None
        col_pos_state = cols[3] if is_block else cols[2]

        # --- Historical baseline (LEFT) ---
        with col_baseline:
            st.markdown("**Historical baseline**")
            if baseline_val_f is not None:
                number_str = format_value(baseline_val_f, units=None)
                st.markdown(
                    _risk_metric_html(
                        number_str=number_str,
                        units=units,
                        help_text=f"Historical baseline: {baseline_desc}",
                    ),
                    unsafe_allow_html=True,
                )
            else:
                st.write("Baseline not available")

        # --- Current value (MIDDLE) + delta pill (below) ---
        with col_current:
            st.markdown("**Current value**")
            if current_val_f is not None:
                number_str = format_value(current_val_f, units=None)
                help_text = f"{variable_label} ({sel_scenario}, {sel_period}, {sel_stat})"
                st.markdown(
                    _risk_metric_html(
                        number_str=number_str,
                        units=units,
                        delta_text=delta_str,
                        delta_kind=delta_kind,
                        help_text=help_text,
                    ),
                    unsafe_allow_html=True,
                )
            else:
                st.write("No data")

        # --- Position in district (block mode only; unchanged position) ---
        if is_block and col_pos_district is not None:
            with col_pos_district:
                st.markdown("**Position in district**")
                if rank_in_district is not None and n_in_district is not None:
                    district_label = parent_district_name or "selected district"
                    rank_1_meaning = "highest" if rank_higher_is_worse else "lowest"
                    if percentile_district is not None:
                        help_text = (
                            f"Percentile (higher = worse): {percentile_district:.0f}th\n"
                            f"Computed among {n_in_district} blocks with available data in {district_label}. "
                            f"Rank 1 = {rank_1_meaning} value."
                        )
                    else:
                        help_text = (
                            f"Computed among {n_in_district} blocks with available data in {district_label}. "
                            f"Rank 1 = {rank_1_meaning} value."
                        )

                    st.markdown(
                        _risk_metric_html(
                            number_str=str(rank_in_district),
                            units=f"/{n_in_district}",
                            help_text=help_text,
                        ),
                        unsafe_allow_html=True,
                    )
                else:
                    st.write("Insufficient data")

        # --- Position in state (RIGHT; keep ↗ button as-is) ---
        with col_pos_state:
            header_cols = st.columns([0.84, 0.16], gap="small")
            with header_cols[0]:
                st.markdown("**Position in state**")

            with header_cols[1]:
                already_rankings = st.session_state.get("active_view") == VIEW_RANKINGS
                if st.button(
                    "↗",
                    key="btn_open_rankings_from_pos_state_header",
                    help="Open rankings table",
                    disabled=already_rankings,
                ):
                    st.session_state["jump_to_rankings"] = True
                    st.session_state["jump_to_map"] = False
                    st.rerun()

            if rank_in_state is not None and n_in_state is not None:
                unit_word = "blocks" if is_block else "districts"
                rank_1_meaning = "highest" if rank_higher_is_worse else "lowest"
                if percentile_state is not None:
                    help_text = (
                        f"Percentile (higher = worse): {percentile_state:.0f}th\n"
                        f"Computed among {n_in_state} {unit_word} with available data in {state_to_show}. "
                        f"Rank 1 = {rank_1_meaning} value."
                    )
                else:
                    help_text = (
                        f"Computed among {n_in_state} {unit_word} with available data in {state_to_show}. "
                        f"Rank 1 = {rank_1_meaning} value."
                    )

                st.markdown(
                    _risk_metric_html(
                        number_str=str(rank_in_state),
                        units=f"/{n_in_state}",
                        help_text=help_text,
                    ),
                    unsafe_allow_html=True,
                )
            else:
                st.write("Insufficient data")


def render_trend_over_time(
    *,
    hist_ts: pd.DataFrame,
    scen_ts: pd.DataFrame,
    variable_label: str,
    sel_scenario: str,
    sel_period: str,
    district_name: str,
    state_dir_for_fs: str,
    district_for_fs: str,
    fig_size_panel: tuple[float, float],
    fig_dpi_panel: int,
    font_size_legend: int,
    units: Optional[str] = None,
    logo_path: Optional[Path] = None,
    create_trend_figure_fn: Callable[..., Any],
) -> None:
    """Render the Trend over time expander with sparkline + narrative."""
    import re
    import streamlit as st

    def _parse_period(s: str) -> tuple[Optional[int], Optional[int]]:
        txt = str(s or "").strip()
        m = re.match(r"^(\d{4})\D+(\d{4})$", txt)
        if not m:
            return None, None
        return int(m.group(1)), int(m.group(2))

    def _period_mean_from_ts(df: pd.DataFrame, start: int, end: int) -> Optional[float]:
        if df is None or df.empty:
            return None
        if "year" not in df.columns or "mean" not in df.columns:
            return None
        tmp = df.copy()
        tmp["year"] = pd.to_numeric(tmp["year"], errors="coerce")
        tmp["mean"] = pd.to_numeric(tmp["mean"], errors="coerce")
        tmp = tmp.dropna(subset=["year", "mean"])
        tmp = tmp[(tmp["year"] >= start) & (tmp["year"] <= end)]
        if tmp.empty:
            return None
        return float(tmp["mean"].mean())

    with st.expander("Trend over time", expanded=False):
        st.caption(
            f"Looking for yearly CSVs under: {state_dir_for_fs} / {district_for_fs} "
            f"(historical + {sel_scenario})"
        )

        if not hist_ts.empty or not scen_ts.empty:
            st.markdown("**Trend over time**")

            # Compute compare mean for hover delta (Δ vs selected period mean)
            p0, p1 = _parse_period(sel_period)
            compare_period_label = str(sel_period or "").strip()
            compare_period_mean: Optional[float] = None
            if p0 is not None and p1 is not None:
                # Choose which series to compute the period mean from
                hist_max = int(pd.to_numeric(hist_ts["year"], errors="coerce").max()) if not hist_ts.empty and "year" in hist_ts.columns else None
                if sel_scenario.strip().lower() == "historical":
                    base_ts = hist_ts
                elif hist_max is not None and p1 <= hist_max:
                    base_ts = hist_ts
                else:
                    base_ts = scen_ts
                compare_period_mean = _period_mean_from_ts(base_ts, p0, p1)

            try:
                fig_ts = create_trend_figure_fn(
                    hist_ts=hist_ts,
                    scen_ts=scen_ts,
                    idx_label=variable_label,
                    scenario_name=sel_scenario,
                    # Plotly function will accept these; Matplotlib function will ignore via fallback
                    compare_period_label=compare_period_label,
                    compare_period_mean=compare_period_mean,
                    units=units,
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

            # Render Plotly if it's a Plotly Figure; otherwise default to pyplot
            try:
                import plotly.graph_objects as go

                if isinstance(fig_ts, go.Figure):
                    st.plotly_chart(fig_ts, use_container_width=True, config={"displaylogo": False, "responsive": True})
                else:
                    st.pyplot(fig_ts, use_container_width=True)
            except Exception:
                st.pyplot(fig_ts, use_container_width=True)


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
            # `make_scenario_figure_fn` may return either:
            # - a Matplotlib figure (or a (fig, ax) tuple)
            # - a Plotly Figure (iterable over traces, so it MUST NOT be unpacked)
            try:
                sc_res = make_scenario_figure_fn(
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
                # Backward-compatible fallback if signature differs
                sc_res = make_scenario_figure_fn(
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

            fig_sc = None
            is_plotly = False
            try:
                import plotly.graph_objects as go

                if isinstance(sc_res, go.Figure):
                    is_plotly = True
                    fig_sc = sc_res
            except Exception:
                pass

            if is_plotly:
                if fig_sc is not None:
                    st.plotly_chart(
                        fig_sc,
                        use_container_width=True,
                        config={"displaylogo": False, "responsive": True},
                    )
            else:
                if isinstance(sc_res, tuple) and len(sc_res) >= 1:
                    fig_sc = sc_res[0]
                else:
                    fig_sc = sc_res

                if fig_sc is not None:
                    try:
                        from india_resilience_tool.viz.style import add_ra_logo

                        add_ra_logo(fig_sc, logo_path)
                    except Exception:
                        pass

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

        # ---------------------------------------------------------------------
        # Bundle-first selection (parity with Portfolio mode)
        # ---------------------------------------------------------------------
        from india_resilience_tool.config.variables import (
            get_bundles,
            get_bundle_for_metric,
            get_bundle_description,
            get_default_bundle,
            get_metrics_for_bundle,
        )

        index_options = list(variables.keys())
        all_bundles = get_bundles()

        bundles_for_current = get_bundle_for_metric(variable_slug)
        default_bundle = bundles_for_current[0] if bundles_for_current else get_default_bundle()

        selected_bundles = st.multiselect(
            "Risk domains to include in the report",
            options=all_bundles,
            default=[default_bundle] if default_bundle in all_bundles else (all_bundles[:1] if all_bundles else []),
            key="case_study_bundle_selection",
            help=(
                "Select one or more risk domains (bundles). Metrics from all selected domains "
                "will be included in the case-study export."
            ),
        )

        expanded_slugs: list[str] = []
        for bundle in selected_bundles:
            for slug in get_metrics_for_bundle(bundle):
                if slug in index_options and slug not in expanded_slugs:
                    expanded_slugs.append(slug)

        if variable_slug in index_options and variable_slug not in expanded_slugs:
            expanded_slugs = [variable_slug] + expanded_slugs

        if not expanded_slugs:
            expanded_slugs = [variable_slug] if variable_slug in index_options else (index_options[:1] if index_options else [])

        st.caption(f"📊 {len(expanded_slugs)} metric(s) selected")

        manual_mode = st.checkbox(
            "Manually refine metric selection",
            value=st.session_state.get("case_study_manual_refinement", False),
            key="case_study_manual_refinement",
            help="Enable to add/remove individual metrics from the case-study report.",
        )

        if manual_mode:
            existing = st.session_state.get("case_study_indices", [])
            default_indices = [s for s in existing if s in index_options] or expanded_slugs

            selected_index_slugs = st.multiselect(
                "Indices to include in the report",
                options=index_options,
                default=default_indices,
                format_func=lambda s: variables.get(s, {}).get("label", s),
                key="case_study_indices",
            )
        else:
            selected_index_slugs = expanded_slugs
            st.session_state["case_study_indices"] = selected_index_slugs

            if selected_bundles:
                with st.expander(f"View {len(selected_index_slugs)} included metrics", expanded=False):
                    for bundle in selected_bundles:
                        slugs_in_bundle = [s for s in get_metrics_for_bundle(bundle) if s in selected_index_slugs]
                        if not slugs_in_bundle:
                            continue
                        desc = get_bundle_description(bundle)
                        st.markdown(f"**{bundle}** — {desc}" if desc else f"**{bundle}**")
                        for slug in slugs_in_bundle:
                            st.caption(f"  • {variables.get(slug, {}).get('label', slug)}")

        # Clear previously built outputs when the selection context changes (avoid stale downloads).
        sig = (
            tuple(selected_index_slugs),
            str(sel_scenario),
            str(sel_period),
            str(sel_stat),
            str(state_to_show),
            str(district_name),
        )
        if st.session_state.get("case_study_signature") != sig:
            st.session_state["case_study_signature"] = sig
            st.session_state.pop("case_study_summary", None)
            st.session_state.pop("case_study_ts", None)
            st.session_state.pop("case_study_panels", None)
            # Reset build-progress UI when the selection signature changes.
            st.session_state.pop("case_study_build_state", None)
            st.session_state.pop("case_study_build_progress", None)
            st.session_state.pop("case_study_build_message", None)


        # Persistent build progress UI (renders next to the build button).
        build_state = st.session_state.get("case_study_build_state", "idle")
        try:
            build_progress = float(st.session_state.get("case_study_build_progress", 0.0) or 0.0)
        except Exception:
            build_progress = 0.0
        build_message = str(st.session_state.get("case_study_build_message", "") or "")

        if not selected_index_slugs:
            st.info("Select at least one index to build the case-study report.")
            st.session_state["case_study_build_state"] = "idle"
            st.session_state["case_study_build_progress"] = 0.0
            st.session_state["case_study_build_message"] = ""
        else:
            col_btn, col_prog = st.columns([0.42, 0.58])
            with col_btn:
                clicked = st.button(
                    "Build case-study data for this district",
                    key="btn_build_case_study",
                    use_container_width=True,
                )

            with col_prog:
                prog_ph = st.empty()
                msg_ph = st.empty()

                def _clamp01(x: float) -> float:
                    try:
                        x = float(x)
                    except Exception:
                        return 0.0
                    return max(0.0, min(1.0, x))

                build_progress = _clamp01(build_progress)

                if build_state in ("running", "complete", "error"):
                    prog_ph.progress(build_progress)
                    if build_state == "complete":
                        msg_ph.success("Completed ✅")
                    elif build_state == "error":
                        msg_ph.error(build_message or "Build failed.")
                    else:
                        pct = int(round(build_progress * 100))
                        msg_ph.caption(f"🕒 {pct}% — {build_message or 'Working…'}")

            if clicked:
                st.session_state["case_study_build_state"] = "running"
                st.session_state["case_study_build_progress"] = 0.0
                st.session_state["case_study_build_message"] = "Starting…"
                prog_ph.progress(0.0)
                msg_ph.caption("🕒 0% — Starting…")

                def _progress_cb(frac: float, msg: str) -> None:
                    frac = _clamp01(frac)
                    msg_clean = str(msg or "").strip()
                    st.session_state["case_study_build_progress"] = frac
                    st.session_state["case_study_build_message"] = msg_clean
                    prog_ph.progress(frac)
                    pct = int(round(frac * 100))
                    if msg_clean:
                        msg_ph.caption(f"🕒 {pct}% — {msg_clean}")
                    else:
                        msg_ph.caption(f"🕒 {pct}%")

                try:
                    with st.spinner("Building case-study data…"):
                        try:
                            summary_df_cs, ts_dict_cs, panel_dict_cs = build_case_study_data_fn(
                                state_name=state_to_show,
                                district_name=district_name,
                                index_slugs=selected_index_slugs,
                                sel_scenario=sel_scenario,
                                sel_period=sel_period,
                                sel_stat=sel_stat,
                                progress_cb=_progress_cb,
                            )
                        except TypeError:
                            # Backward compatibility: older builders may not accept progress_cb.
                            summary_df_cs, ts_dict_cs, panel_dict_cs = build_case_study_data_fn(
                                state_name=state_to_show,
                                district_name=district_name,
                                index_slugs=selected_index_slugs,
                                sel_scenario=sel_scenario,
                                sel_period=sel_period,
                                sel_stat=sel_stat,
                            )
                except Exception as e:
                    st.session_state["case_study_build_state"] = "error"
                    st.session_state["case_study_build_message"] = str(e)
                    msg_ph.error("Build failed.")
                    st.exception(e)
                else:
                    if summary_df_cs is None or summary_df_cs.empty:
                        st.session_state["case_study_build_state"] = "error"
                        st.session_state["case_study_build_message"] = (
                            "No data found for the selected index/district combination."
                        )
                        msg_ph.error("No data found.")
                        st.warning(
                            "No data found for the selected index/district combination. "
                            "Try including fewer indices or a different scenario/period/statistic."
                        )
                    else:
                        st.session_state["case_study_summary"] = summary_df_cs
                        st.session_state["case_study_ts"] = ts_dict_cs
                        st.session_state["case_study_panels"] = panel_dict_cs

                        st.session_state["case_study_build_state"] = "complete"
                        st.session_state["case_study_build_progress"] = 1.0
                        st.session_state["case_study_build_message"] = "Completed."
                        prog_ph.progress(1.0)
                        msg_ph.success("Completed ✅")
                        if hasattr(st, "toast"):
                            st.toast("Case-study data build completed ✅")

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
    rank_higher_is_worse: bool = True,
    # Time series data
    hist_ts: pd.DataFrame,
    scen_ts: pd.DataFrame,
    # Schema for scenario comparison
    schema_items: Sequence[Mapping[str, Any]],
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
    # Callable dependencies (injected for testability)
    create_trend_figure_fn: Callable[..., Any],
    build_scenario_panel_fn: Callable[..., pd.DataFrame],
    make_scenario_figure_fn: Callable[..., tuple[Any, Any]],
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
    Render the complete single-unit details panel (right column).

    This is the main entry point for the details panel, composing all sub-renderers.

    Contract:
    - Preserves all widget keys from the legacy dashboard
    - Preserves all session_state keys
    - Takes explicit inputs rather than relying on globals
    """
    import streamlit as st

    variable_label = variables.get(variable_slug, {}).get("label", variable_slug)
    units = (
        variables.get(variable_slug, {}).get("unit")
        or variables.get(variable_slug, {}).get("units")
        or ""
    )

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


    # 1. Risk summary
    render_risk_summary(
        current_val_f=current_val_f,
        baseline_val_f=baseline_val_f,
        baseline_col=baseline_col,
        rank_in_state=rank_in_state,
        n_in_state=n_in_state,
        percentile_state=percentile_state,
        rank_higher_is_worse=rank_higher_is_worse,
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
        units=units,
    )

    # 2. Trend over time
    render_trend_over_time(
        hist_ts=hist_ts,
        scen_ts=scen_ts,
        variable_label=variable_label,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        district_name=district_name,
        state_dir_for_fs=state_dir_for_fs or state_to_show,
        district_for_fs=district_for_fs or district_name,
        fig_size_panel=fig_size_panel_169,
        fig_dpi_panel=fig_dpi_panel,
        font_size_legend=font_size_legend,
        units=units,
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

    # 4. Case study export (multi-index)
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

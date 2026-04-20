"""Hydro basin summary panel for basin-wide sub-basin selections."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping, Optional

import pandas as pd

from india_resilience_tool.viz.formatting import format_metric_value, get_metric_display_units

from india_resilience_tool.app.views.state_summary_view import (
    _compute_position_in_india,
    _find_baseline_column,
    _normalize_name,
    _parse_metric_parts,
    _weighted_mean,
    _with_area_weights,
)


def _with_hydro_weights(gdf: Any) -> pd.DataFrame:
    """Return a copy of hydro rows with a stable `__area_m2` weight column."""
    out = pd.DataFrame(gdf).copy()
    for column in ("subbasin_area_km2", "basin_area_km2"):
        if column not in out.columns:
            continue
        weights = pd.to_numeric(out[column], errors="coerce")
        if weights.notna().any() and (weights > 0).any():
            out["__area_m2"] = weights.fillna(0.0) * 1_000_000.0
            return out
    return _with_area_weights(gdf)


def _build_hydro_scenario_panel(
    *,
    df: pd.DataFrame,
    base_metric: str,
    sel_stat: str,
) -> pd.DataFrame:
    """Build a weighted scenario/period/value panel from hydro master columns."""
    from india_resilience_tool.viz.charts import (
        SCENARIO_DISPLAY,
        canonical_period_label,
        ordered_period_keys,
        ordered_scenario_keys,
    )

    records: list[dict[str, object]] = []
    stat_norm = str(sel_stat or "").strip().lower()
    for column in df.columns:
        metric, scenario, period, stat = _parse_metric_parts(column)
        if metric != base_metric or str(stat).strip().lower() != stat_norm:
            continue
        value = _weighted_mean(df, column)
        if value is None:
            continue
        scenario_norm = str(scenario).strip().lower()
        period_norm = canonical_period_label(period)
        records.append(
            {
                "scenario": scenario_norm,
                "period": period_norm,
                "value": float(value),
                "column": column,
            }
        )

    if not records:
        return pd.DataFrame()

    panel_df = pd.DataFrame(records)
    panel_df["scenario_display"] = (
        panel_df["scenario"].astype(str).map(SCENARIO_DISPLAY).fillna(panel_df["scenario"])
    )
    panel_df["period"] = pd.Categorical(
        panel_df["period"],
        categories=ordered_period_keys(panel_df["period"].astype(str).tolist()),
        ordered=True,
    )
    panel_df["scenario"] = pd.Categorical(
        panel_df["scenario"],
        categories=ordered_scenario_keys(panel_df["scenario"].astype(str).tolist()),
        ordered=True,
    )
    panel_df = panel_df.sort_values(["period", "scenario"]).reset_index(drop=True)
    return panel_df


def _period_mean_from_ts(df: pd.DataFrame, start: int, end: int) -> Optional[float]:
    """Return the mean of a trend slice between two years, inclusive."""
    if df is None or df.empty or "year" not in df.columns or "mean" not in df.columns:
        return None
    tmp = df.copy()
    tmp["year"] = pd.to_numeric(tmp["year"], errors="coerce")
    tmp["mean"] = pd.to_numeric(tmp["mean"], errors="coerce")
    tmp = tmp.dropna(subset=["year", "mean"])
    tmp = tmp[(tmp["year"] >= start) & (tmp["year"] <= end)]
    if tmp.empty:
        return None
    return float(tmp["mean"].mean())


def _parse_period_bounds(period: str) -> tuple[Optional[int], Optional[int]]:
    """Parse a `YYYY-YYYY` period string into integer bounds."""
    match = re.match(r"^(\d{4})\D+(\d{4})$", str(period or "").strip())
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def render_hydro_summary_view(
    *,
    selected_basin: str,
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    metric_col: str,
    merged_gdf: Any,
    processed_root: Path,
) -> None:
    """Render a basin-level hydro summary using all visible sub-basins."""
    import streamlit as st

    from india_resilience_tool.analysis.timeseries import load_hydro_yearly
    from india_resilience_tool.viz.charts import (
        create_trend_figure_for_index_plotly,
        make_scenario_comparison_figure_plotly,
    )

    varcfg = variables.get(variable_slug, {}) or {}
    variable_label = varcfg.get("label", variable_slug)
    units = get_metric_display_units(
        metric_slug=variable_slug,
        units=varcfg.get("units") or varcfg.get("unit"),
    )
    rank_higher_is_worse = bool(varcfg.get("rank_higher_is_worse", True))
    supports_yearly_trend = bool(varcfg.get("supports_yearly_trend", True))

    st.subheader(f"{selected_basin} — Basin Climate Profile")
    st.markdown(f"**Index:** {variable_label}  \n**Scenario:** {sel_scenario}  \n**Period:** {sel_period}")

    df_all = _with_hydro_weights(merged_gdf)
    if "basin_name" not in df_all.columns:
        st.info("Basin summary is unavailable because the loaded hydro data does not include basin names.")
        return

    basin_key = _normalize_name(selected_basin)
    df_basin = df_all[
        df_all["basin_name"].astype(str).str.strip().str.lower() == basin_key
    ].copy()
    if df_basin.empty:
        st.info("No hydro rows were found for the selected basin.")
        return

    base_metric, _, _, _ = _parse_metric_parts(metric_col)
    baseline_col = _find_baseline_column(list(df_all.columns), base_metric)
    current_val = _weighted_mean(df_basin, metric_col)
    baseline_val = _weighted_mean(df_basin, baseline_col)

    basin_value_map: dict[str, float] = {}
    for basin_name, grp in df_all.groupby("basin_name"):
        basin_val = _weighted_mean(grp, metric_col)
        if basin_val is not None:
            basin_value_map[_normalize_name(basin_name)] = float(basin_val)
    rank_india, n_india = _compute_position_in_india(
        basin_value_map,
        selected_basin,
        higher_is_worse=rank_higher_is_worse,
    )

    with st.expander("Risk summary", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Historical baseline**")
            st.metric(
                "",
                format_metric_value(baseline_val, metric_slug=variable_slug, units=units)
                if baseline_val is not None
                else "N/A",
            )
        with c2:
            st.markdown("**Current value**")
            delta = (current_val - baseline_val) if (current_val is not None and baseline_val is not None) else None
            st.metric(
                "",
                format_metric_value(current_val, metric_slug=variable_slug, units=units)
                if current_val is not None
                else "N/A",
                f"{delta:+.2f}" if delta is not None else None,
            )
        with c3:
            st.markdown("**Position among basins**")
            if rank_india is not None and n_india >= 2:
                st.metric("", f"#{rank_india} / {n_india}")
            else:
                st.metric("", "N/A")
                st.caption("Insufficient basins to rank")

    with st.expander("Trend over time (basin average)", expanded=False):
        if not supports_yearly_trend:
            st.caption("Yearly trend charts are not available for this source yet.")
        else:
            hist_ts = load_hydro_yearly(
                ts_root=processed_root,
                level="basin",
                basin_display=selected_basin,
                subbasin_display=None,
                scenario_name="historical",
            )
            scen_ts = load_hydro_yearly(
                ts_root=processed_root,
                level="basin",
                basin_display=selected_basin,
                subbasin_display=None,
                scenario_name=sel_scenario,
            )

            if hist_ts.empty and scen_ts.empty:
                st.info("Hydro yearly data not available for this basin and metric.")
            else:
                p0, p1 = _parse_period_bounds(sel_period)
                compare_period_mean: Optional[float] = None
                if p0 is not None and p1 is not None:
                    if str(sel_scenario).strip().lower() == "historical":
                        compare_period_mean = _period_mean_from_ts(hist_ts, p0, p1)
                    else:
                        compare_period_mean = _period_mean_from_ts(scen_ts, p0, p1)

                fig_ts = create_trend_figure_for_index_plotly(
                    hist_ts=hist_ts,
                    scen_ts=scen_ts,
                    idx_label=variable_label,
                    scenario_name=sel_scenario,
                    compare_period_label=str(sel_period or "").strip(),
                    compare_period_mean=compare_period_mean,
                    units=units,
                    render_context="dashboard",
                )
                if fig_ts is None:
                    st.info("Hydro yearly data is available, but the trend figure could not be rendered.")
                else:
                    st.plotly_chart(
                        fig_ts,
                        use_container_width=True,
                        config={"displaylogo": False, "responsive": True},
                    )

    with st.expander("Scenario comparison (period-mean)", expanded=False):
        panel_df = _build_hydro_scenario_panel(
            df=df_basin,
            base_metric=base_metric,
            sel_stat=sel_stat,
        )
        if panel_df.empty:
            st.info("Scenario comparison is not available for this basin and metric.")
        else:
            fig_sc = make_scenario_comparison_figure_plotly(
                panel_df=panel_df,
                metric_label=variable_label,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
                district_name=selected_basin,
                units=units,
                render_context="dashboard",
            )
            if fig_sc is None:
                st.info("Scenario comparison data is available, but the figure could not be rendered.")
            else:
                st.plotly_chart(
                    fig_sc,
                    use_container_width=True,
                    config={"displaylogo": False, "responsive": True},
                )
